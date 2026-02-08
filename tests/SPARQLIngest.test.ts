import { describe, test, expect, afterAll, beforeAll, afterEach } from "vitest";
import { fastify } from "fastify";
import { readFile } from "fs/promises";
import { channel, createRunner } from "@rdfc/js-runner/lib/testUtils"
import * as winston from 'winston'
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";
import { Writer as N3Writer, Parser } from "n3";
import { QueryEngine } from "@comunica/query-sparql";
import { SPARQLIngest, OperationMode } from "../src/SPARQLIngest";
import { SDS } from "@treecg/types";
import { consumeOutput } from "./Utils";

import type { FullProc, Reader } from "@rdfc/js-runner";
import type { FastifyInstance } from "fastify";
import type { IngestConfig } from "../src/SPARQLIngest";

const df = new DataFactory();

describe("Functional tests for the sparqlIngest RDF-Connect function", () => {

    const ENTITY_SHAPE = `
        @prefix sh: <http://www.w3.org/ns/shacl#>.
        @prefix ex:  <https://example.org/ns#>.

        [ ] a sh:NodeShape;
            sh:targetClass ex:Entity;
            sh:property [
                sh:path ex:prop2;
                sh:node [
                    a sh:NodeShape;
                    sh:targetClass ex:NestedEntity
                ]
            ].
    `;

    let server: FastifyInstance;
    let reqCount = 0;

    const logger = winston.createLogger({
        transports: [new winston.transports.Console()],
        level: "debug"
    });

    beforeAll(async () => {
        // Setup mock http server
        try {
            server = fastify({ bodyLimit: 5 * 1024 * 1024 });
            // Add support for application/x-www-form-urlencoded
            server.addContentTypeParser(
                "application/x-www-form-urlencoded",
                { parseAs: 'string' },
                (req, body, done) => {
                    done(null, body);
                }
            );
            server.addContentTypeParser(
                "application/n-quads",
                { parseAs: 'string' },
                (req, body, done) => {
                    done(null, body);
                }
            );
            await server.register(async (fastify) => {
                fastify.post("/sparql", async (req, res) => {
                    // Keep track of the number of requests
                    reqCount++;
                    res.send("OK");
                });
            });
            await server.listen({ port: 3000 });
            console.log(
                `Mock server listening on ${server.addresses()[0].port}`,
            );
        } catch (err) {
            console.error(err);
            process.exit(1);
        }
    });

    afterEach(() => {
        reqCount = 0;
    });

    afterAll(async () => {
        await server.close();
    });

    test("SDS Member INSERT into a SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Local RDF store and SPARQL engine to verify the results
        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        consumeOutput(memberStreamReader, async (query) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });

            // Query the triple store to verify that triples were inserted
            const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                sources: [localStore],
            });

            let sawEntity = false;
            let sawProp2 = false;
            let sawNestedType = false;

            for await (const bindings of stream) {
                const s = bindings.get("s");
                const p = bindings.get("p");
                const o = bindings.get("o");

                if (s?.value === "https://example.org/entity/Entity_0") {
                    sawEntity = true;
                }
                if (p?.value === "https://example.org/ns#prop2") {
                    sawProp2 = true;
                }
                if (o?.value === "https://example.org/ns#NestedEntity") {
                    sawNestedType = true;
                }
            }

            expect(sawEntity).toBeTruthy();
            expect(sawProp2).toBeTruthy();
            expect(sawNestedType).toBeTruthy();

            // Close the member stream
            await memberStreamWriter.close();
        });

        const config: IngestConfig = {
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            }
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);
        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();
        await memberStreamWriter.string(
            dataGenerator({
                changeType: config.changeSemantics!.createValue,
                includeAllProps: true,
                withMetadata: true,
            }
            ));

        // Wait until the processing is done
        await processingPromise;
    });

    test("Default SDS Member DELETE/INSERT into a populated SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Add some data to the local triple store first
        const localStore = RdfStore.createDefault();
        new Parser().parse(dataGenerator({
            includeAllProps: true,
            includeBlankNodes: true,
        })).forEach(quad => localStore.addQuad(quad));
        const myEngine = new QueryEngine();

        consumeOutput(memberStreamReader, async (query) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });

            // Query the triple store to verify that triples were updated properly
            const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                sources: [localStore],
            });

            let sawEntity = false;
            let sawProp2 = false;
            let sawNestedType = false;
            let sawNewProp = false;

            for await (const bindings of stream) {
                const s = bindings.get("s");
                const p = bindings.get("p");
                const o = bindings.get("o");

                if (s?.value === "https://example.org/entity/Entity_0") {
                    sawEntity = true;
                }
                if (p?.value === "https://example.org/ns#prop2") {
                    sawProp2 = true;
                }
                if (p?.value === "https://example.org/ns#newProp") {
                    sawNewProp = true;
                }
                if (o?.value === "https://example.org/ns#NestedEntity") {
                    sawNestedType = true;
                }
            }

            expect(sawEntity).toBeTruthy();
            expect(sawProp2).toBeTruthy();
            expect(sawNewProp).toBeTruthy();
            expect(sawNestedType).toBeTruthy();

            // Close the member stream
            await memberStreamWriter.close();
        });

        const config: IngestConfig = {};

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        // Prepare updated member
        const store = RdfStore.createDefault();
        new Parser().parse(dataGenerator({
            withMetadata: true,
            includeAllProps: true,
            includeBlankNodes: true,
        })).forEach(quad => store.addQuad(quad));
        store.addQuad(
            df.quad(
                df.namedNode("https://example.org/entity/Entity_0"),
                df.namedNode("https://example.org/ns#newProp"),
                df.literal("Updated value")
            )
        );

        memberStreamWriter.string(new N3Writer().quadsToString(store.getQuads()));
        // Wait until the processing is done
        await processingPromise;
    });

    test("Default SDS Member DELETE/INSERT into an empty SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Local RDF store and SPARQL engine to verify the results
        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        consumeOutput(memberStreamReader, async (query) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });

            // Query the triple store to verify that triples were updated properly
            const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                sources: [localStore],
            });

            let sawEntity = false;
            let sawProp2 = false;
            let sawNestedType = false;
            let sawNewProp = false;

            for await (const bindings of stream) {
                const s = bindings.get("s");
                const p = bindings.get("p");
                const o = bindings.get("o");

                if (s?.value === "https://example.org/entity/Entity_0") {
                    sawEntity = true;
                }
                if (p?.value === "https://example.org/ns#prop2") {
                    sawProp2 = true;
                }
                if (p?.value === "https://example.org/ns#newProp") {
                    sawNewProp = true;
                }
                if (o?.value === "https://example.org/ns#NestedEntity") {
                    sawNestedType = true;
                }
            }

            expect(sawEntity).toBeTruthy();
            expect(sawProp2).toBeTruthy();
            expect(sawNewProp).toBeTruthy();
            expect(sawNestedType).toBeTruthy();

            // Close the member stream
            await memberStreamWriter.close();
        });

        const config: IngestConfig = {};

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        // Prepare updated member
        const store = RdfStore.createDefault();
        new Parser().parse(dataGenerator({ withMetadata: true, includeAllProps: true }))
            .forEach(quad => store.addQuad(quad));
        store.addQuad(
            df.quad(
                df.namedNode("https://example.org/entity/Entity_0"),
                df.namedNode("https://example.org/ns#newProp"),
                df.literal("Updated value")
            )
        );

        memberStreamWriter.string(new N3Writer().quadsToString(store.getQuads()));
        await processingPromise;
    });

    test("Default SDS Member DELETE/INSERT into an empty SPARQL endpoint having a large member insert", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Local RDF store and SPARQL engine to verify the results
        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        let queryCount = 0;
        consumeOutput(memberStreamReader, async (query) => {
            queryCount++;
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });
        });

        const config: IngestConfig = {
            forVirtuoso: true,
            graphStoreUrl: "http://localhost:3000/sparql",
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();
        await memberStreamWriter.string(await readFile("./tests/data/large-member.nq", "utf-8"));
        // Close the member stream
        await memberStreamWriter.close();

        await processingPromise;

        expect(queryCount).toBe(5);
        // Query the triple store to verify that triples were updated properly
        const stream = await myEngine.queryBindings("SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }", {
            sources: [localStore],
        });

        for await (const bindings of stream) {
            const count = bindings.get("count");
            expect(parseInt(count!.value)).toBe(1828);
        }
        // Check that the number of requests made to the mock server is correct
        expect(reqCount).toBe(1);
    });

    test("Default non-SDS named graph Member DELETE/INSERT into an empty SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Local RDF store and SPARQL engine to verify the results
        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        consumeOutput(memberStreamReader, async (query: string) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });
            // Query the triple store to verify that quads were updated properly
            const stream = await myEngine.queryBindings("SELECT (COUNT(*) AS ?count) WHERE { GRAPH ?g { ?s ?p ?o } }", {
                sources: [localStore],
            });
            for await (const bindings of stream) {
                const count = bindings.get("count");
                expect(parseInt(count!.value)).toBe(38);
            }

            // Close the member stream
            await memberStreamWriter.close();
        });

        const config: IngestConfig = {
            graphStoreUrl: "http://localhost:3000/sparql",
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        memberStreamWriter.string(await readFile("./tests/data/named-graph-member.nq", "utf-8"));
        await processingPromise;

        // Check that the number of requests made to the mock server is correct
        expect(reqCount).toBe(1);
    });

    test("Default SDS named graph Member DELETE/INSERT into an empty SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Local RDF store and SPARQL engine to verify the results
        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        consumeOutput(memberStreamReader, async (query: string) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });
            // Query the triple store to verify that quads were updated properly
            const stream = await myEngine.queryBindings("SELECT (COUNT(*) AS ?count) WHERE { GRAPH ?g { ?s ?p ?o } }", {
                sources: [localStore],
            });
            for await (const bindings of stream) {
                const count = bindings.get("count");
                expect(parseInt(count!.value)).toBe(38);
            }

            // Close the member stream
            await memberStreamWriter.close();
        });

        const config: IngestConfig = {
            graphStoreUrl: "http://localhost:3000/sparql",
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        memberStreamWriter.string(await readFile("./tests/data/sds-named-graph-member.nq", "utf-8"));
        await processingPromise;

        // Check that the number of requests made to the mock server is correct
        expect(reqCount).toBe(1);
    });

    test("Default SDS Member DELETE/INSERT into an empty SPARQL endpoint having a very large member insert", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Local RDF store and SPARQL engine to verify the results
        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        let queryCount = 0;
        consumeOutput(memberStreamReader, async (query) => {
            queryCount++;
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });
        });

        const config: IngestConfig = {
            forVirtuoso: true,
            graphStoreUrl: "http://localhost:3000/sparql",
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        await memberStreamWriter.string(await readFile("./tests/data/very-large-member.nq", "utf-8"));
        // Close the member stream
        await memberStreamWriter.close();
        await processingPromise;

        // Check that the number of requests made to the mock server is correct
        expect(reqCount).toBe(24);
        expect(queryCount).toBe(24);

        // Query the triple store to verify that triples were updated properly
        const stream = await myEngine.queryBindings("SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }", {
            sources: [localStore],
        });

        for await (const bindings of stream) {
            const count = bindings.get("count");
            expect(parseInt(count!.value)).toBe(11083);
        }
    });

    test("SDS Member DELETE without shape and including all properties in a SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Add some data to the triple store first
        const localStore = RdfStore.createDefault();
        new Parser().parse(dataGenerator({
            includeAllProps: true,
            includeBlankNodes: true,
        })).forEach(quad => localStore.addQuad(quad));
        const myEngine = new QueryEngine();

        consumeOutput(memberStreamReader, async (query) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });

            // Query the triple store to verify that triples were deleted properly
            const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                sources: [localStore],
            });

            let counter = 0;

            for await (const bindings of stream) {
                const s = bindings.get("s");
                const p = bindings.get("p");
                const o = bindings.get("o");
                counter++;
            }

            expect(counter).toBe(0);

            // Close the member stream
            await memberStreamWriter.close();
        });

        const config: IngestConfig = {
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            }
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        memberStreamWriter.string(
            dataGenerator({
                changeType: config.changeSemantics!.deleteValue,
                includeAllProps: true,
                includeBlankNodes: true,
                withMetadata: true,
            })
        );

        await processingPromise;
    });

    test("SDS Member DELETE with declared shape in a SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Add some initial data to the triple store
        const localStore = RdfStore.createDefault();
        new Parser().parse(dataGenerator({ includeAllProps: true })).forEach(quad => localStore.addQuad(quad));
        const myEngine = new QueryEngine();

        consumeOutput(memberStreamReader, async (query) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });

            // Query the triple store to verify that triples were deleted properly
            const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                sources: [localStore],
            });

            let counter = 0;

            for await (const _ of stream) {
                counter++;
            }

            expect(counter).toBe(0);

            // Close the member stream
            await memberStreamWriter.close();
        });

        const config: IngestConfig = {
            memberShape: ENTITY_SHAPE,
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            }
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        memberStreamWriter.string(
            dataGenerator({
                changeType: config.changeSemantics!.deleteValue,
                includeAllProps: false,
                withMetadata: true,
            })
        );

        await processingPromise;
    });

    test("SDS Member DELETE with declared shape and no type in a SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        const localStore = RdfStore.createDefault();
        new Parser().parse(dataGenerator({ includeAllProps: true })).forEach(quad => localStore.addQuad(quad));
        const myEngine = new QueryEngine();

        consumeOutput(memberStreamReader, async (query) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });

            // Query the triple store to verify that triples were deleted properly
            const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                sources: [localStore],
            });

            let counter = 0;

            for await (const bindings of stream) {
                counter++;
            }

            expect(counter).toBe(0);

            // Close the member stream
            await memberStreamWriter.close();
        });

        const config: IngestConfig = {
            memberShape: ENTITY_SHAPE,
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            }
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        // Prepare property-less (only type and change type) member
        const store = RdfStore.createDefault();
        store.addQuad(
            df.quad(
                df.blankNode(),
                df.namedNode(SDS.stream),
                df.namedNode("https://example.org/ns#sdsStream"),
                df.namedNode(SDS.custom("DataDescription"))
            )
        );
        store.addQuad(
            df.quad(
                df.blankNode(),
                df.namedNode(SDS.payload),
                df.namedNode("https://example.org/entity/Entity_0"),
                df.namedNode(SDS.custom("DataDescription"))
            )
        );
        store.addQuad(
            df.quad(
                df.namedNode("https://example.org/entity/Entity_0"),
                df.namedNode("https://example.org/ns#changeType"),
                df.namedNode("https://example.org/ns#Delete")
            )
        );

        memberStreamWriter.string(new N3Writer().quadsToString(store.getQuads()));

        await processingPromise;
    });

    test("Default DELETE/INSERT for non-SDS data into an empty SPARQL endpoint having a large member insert", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Local RDF store and SPARQL engine to verify the results
        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        let queryCount = 0;
        consumeOutput(memberStreamReader, async (query) => {
            queryCount++;
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });
        });

        const config: IngestConfig = {
            forVirtuoso: true,
            graphStoreUrl: "http://localhost:3000/sparql",
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        await memberStreamWriter.string((await readFile("./tests/data/non-sds-data.nq", "utf-8")));
        await memberStreamWriter.close();
        await processingPromise;

        expect(queryCount).toBe(5);
        // Query the triple store to verify that triples were updated properly
        const stream = await myEngine.queryBindings("SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }", {
            sources: [localStore],
        });

        for await (const bindings of stream) {
            const count = bindings.get("count");
            expect(parseInt(count!.value)).toBe(1828);
        }

        // Check that the number of requests made to the mock server is correct
        expect(reqCount).toBe(1);
    });

    test("Default DELETE/INSERT for non-SDS data into an empty named graph in a SPARQL endpoint having a large member insert", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        // Local RDF store and SPARQL engine to verify the results
        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        let queryCount = 0;
        consumeOutput(memberStreamReader, async (query) => {
            queryCount++;
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });
        });

        const config: IngestConfig = {
            forVirtuoso: true,
            graphStoreUrl: "http://localhost:3000/sparql",
            targetNamedGraph: "http://example.org/graph"
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        await memberStreamWriter.string((await readFile("./tests/data/non-sds-data.nq", "utf-8")));
        // Close the member stream
        await memberStreamWriter.close();

        await processingPromise;

        expect(queryCount).toBe(5);
        // Query the triple store to verify that triples were updated properly
        const stream = await myEngine.queryBindings("SELECT (COUNT(*) AS ?count) WHERE { GRAPH <http://example.org/graph> {?s ?p ?o} }", {
            sources: [localStore],
        });

        for await (const bindings of stream) {
            const count = bindings.get("count");
            expect(parseInt(count!.value)).toBe(1828);
        }

        // Check that the number of requests made to the mock server is correct
        expect(reqCount).toBe(1);
    });

    test("Replication mode: SDS data into an empty SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        consumeOutput(memberStreamReader, async (query) => {
            expect(query).toBeDefined();
        });

        const config: IngestConfig = {
            operationMode: OperationMode.REPLICATION,
            graphStoreUrl: "http://localhost:3000/sparql",
            targetNamedGraph: "http://example.org/graph",
            memberBatchSize: 5
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        await sparqlIngest.init();
        const processingPromise = sparqlIngest.transform();

        // Send more than batch size to trigger batch and have leftovers for flush
        const member = await readFile("./tests/data/large-member.nq", "utf-8");
        for (let i = 0; i < 6; i++) {
            await memberStreamWriter.string(member);
        }

        // Close the member stream
        await memberStreamWriter.close();

        expect(reqCount).toBe(1);
        await memberStreamWriter.close();
        await processingPromise;

        // Check that the number of requests made to the mock server is correct
        expect(reqCount).toBe(2);
    });

    test("Replication mode: Non-SDS data into an empty SPARQL endpoint", async () => {
        const runner = createRunner();
        const [memberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        consumeOutput(memberStreamReader, async (query) => {
            expect(query).toBeDefined();
        });

        const checkOutput = async (reader: Reader) => {
            for await (const query of reader.strings()) {

            }
        };

        checkOutput(memberStreamReader);

        const config: IngestConfig = {
            operationMode: OperationMode.REPLICATION,
            graphStoreUrl: "http://localhost:3000/sparql",
            targetNamedGraph: "http://example.org/graph",
            memberBatchSize: 5
        };

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        await sparqlIngest.init();
        const processingPromise = sparqlIngest.transform();

        // Send more than batch size to trigger batch and have leftovers for flush
        const member = await readFile("./tests/data/non-sds-data.nq", "utf-8");
        for (let i = 0; i < 6; i++) {
            await memberStreamWriter.string(member);
        }
        // Close the member stream
        await memberStreamWriter.close();

        expect(reqCount).toBe(1);
        await processingPromise;

        // Check that the number of requests made to the mock server is correct
        expect(reqCount).toBe(2);
    });

    test("Transaction-aware SDS Member ingestion into a SPARQL endpoint (with shape description for deletes)", async () => {
        const runner = createRunner();
        const [mmeberStreamWriter, memberStream] = channel(runner, "members");
        const [sparqlWriter, memberStreamReader] = channel(runner, "queries");

        consumeOutput(memberStreamReader, async (query) => {
            // Execute produced SPARQL query
            await myEngine.queryVoid(query, {
                sources: [localStore],
            });

            /**
             * Whe should see that:
             * - ex:Entity_0 and ex:Entity_1 were updated
             * - ex:Entity_4 and ex:Entity_5 were created
             * - ex:Entity_2 and ex:Entity_3 were deleted
             */
            // Query the triple store to verify that triples were updated properly
            const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                sources: [localStore],
            });

            let sawEntity0 = false;
            let sawEntity1 = false;
            let sawEntity2 = false;
            let sawEntity3 = false;
            let sawEntity4 = false;
            let sawEntity5 = false;
            let sawNewPropIn0 = false;
            let sawNewPropIn1 = false;

            for await (const bindings of stream) {
                const s = bindings.get("s");
                const p = bindings.get("p");
                const o = bindings.get("o");

                if (s?.value === "https://example.org/entity/Entity_0") {
                    sawEntity0 = true;
                }
                if (s?.value === "https://example.org/entity/Entity_1") {
                    sawEntity1 = true;
                }
                if (s?.value === "https://example.org/entity/Entity_2") {
                    sawEntity1 = true;
                }
                if (s?.value === "https://example.org/entity/Entity_3") {
                    sawEntity1 = true;
                }
                if (s?.value === "https://example.org/entity/Entity_4") {
                    sawEntity4 = true;
                }
                if (s?.value === "https://example.org/entity/Entity_5") {
                    sawEntity5 = true;
                }
                if (s?.value === "https://example.org/entity/Entity_0"
                    && p?.value === "https://example.org/ns#newProp"
                    && o?.value === "Updated value") {
                    sawNewPropIn0 = true;
                }
                if (s?.value === "https://example.org/entity/Entity_1"
                    && p?.value === "https://example.org/ns#newProp"
                    && o?.value === "Updated value") {
                    sawNewPropIn1 = true;
                }
            }

            expect(sawEntity0).toBeTruthy();
            expect(sawEntity1).toBeTruthy();
            expect(sawEntity2).toBeFalsy();
            expect(sawEntity3).toBeFalsy();
            expect(sawEntity4).toBeTruthy();
            expect(sawEntity5).toBeTruthy();
            expect(sawNewPropIn0).toBeTruthy();
            expect(sawNewPropIn1).toBeTruthy();
        });

        const config: IngestConfig = {
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            },
            memberShape: ENTITY_SHAPE,
            transactionConfig: {
                transactionIdPath: "https://w3id.org/ldes#transactionId",
                transactionEndPath: "https://w3id.org/ldes#isLastOfTransaction"
            }
        };

        // Add some data to the triple store first (ex:Entity_0, ex:Entity_1, ex:Entity_2 and ex:Entity_3)
        const localStore = RdfStore.createDefault();
        ["0", "1", "2", "3"].forEach(index => {
            new Parser().parse(dataGenerator({
                includeAllProps: true,
                memberIndex: index,
                withMetadata: false,
            })).forEach(quad => localStore.addQuad(quad));
        });

        const myEngine = new QueryEngine();

        // Execute processor function
        const sparqlIngest = <FullProc<SPARQLIngest>>new SPARQLIngest({
            memberStream,
            config,
            sparqlWriter
        }, logger);

        // Initialize and start the processor
        await sparqlIngest.init();
        // Start the processing function
        const processingPromise = sparqlIngest.transform();

        await mmeberStreamWriter.string(
            dataGenerator({
                changeType: config.changeSemantics!.createValue,
                memberIndex: "4",
                includeAllProps: true,
                withMetadata: true,
                isPartOfTransaction: true,
                isLastOfTransaction: false,
            })
        )

        await mmeberStreamWriter.string(
            dataGenerator({
                changeType: config.changeSemantics!.createValue,
                memberIndex: "5",
                includeAllProps: true,
                withMetadata: true,
                isPartOfTransaction: true,
                isLastOfTransaction: false,
            })
        )

        // Update members ex:Entity_0 and ex:Entity_1
        const updateStore0 = RdfStore.createDefault();
        new Parser().parse(dataGenerator({
            changeType: config.changeSemantics!.updateValue,
            memberIndex: "0",
            includeAllProps: true,
            withMetadata: true,
            isPartOfTransaction: true,
            isLastOfTransaction: false
        })).forEach(quad => updateStore0.addQuad(quad));

        updateStore0.addQuad(
            df.quad(
                df.namedNode("https://example.org/entity/Entity_0"),
                df.namedNode("https://example.org/ns#newProp"),
                df.literal("Updated value")
            )
        );

        await mmeberStreamWriter.string(
            new N3Writer().quadsToString(updateStore0.getQuads())
        );

        const updateStore1 = RdfStore.createDefault();
        new Parser().parse(dataGenerator({
            changeType: config.changeSemantics!.updateValue,
            memberIndex: "1",
            includeAllProps: true,
            withMetadata: true,
            isPartOfTransaction: true,
            isLastOfTransaction: false
        })).forEach(quad => updateStore1.addQuad(quad));

        updateStore1.addQuad(
            df.quad(
                df.namedNode("https://example.org/entity/Entity_1"),
                df.namedNode("https://example.org/ns#newProp"),
                df.literal("Updated value")
            )
        );

        await mmeberStreamWriter.string(new N3Writer().quadsToString(updateStore1.getQuads()));

        await mmeberStreamWriter.string(
            dataGenerator({
                changeType: config.changeSemantics!.deleteValue,
                memberIndex: "2",
                includeAllProps: false,
                withMetadata: true,
                isPartOfTransaction: true,
                isLastOfTransaction: false
            })
        );

        await mmeberStreamWriter.string(
            dataGenerator({
                changeType: config.changeSemantics!.deleteValue,
                memberIndex: "3",
                includeAllProps: false,
                withMetadata: true,
                isPartOfTransaction: true,
                isLastOfTransaction: true
            })
        );

        await mmeberStreamWriter.close();

        await processingPromise;
    });
});

function dataGenerator(props: {
    changeType?: string,
    memberIndex?: string,
    withMetadata?: boolean,
    includeAllProps?: boolean,
    includeBlankNodes?: boolean,
    isPartOfTransaction?: boolean,
    isLastOfTransaction?: boolean,
    memberIsGraph?: boolean,
}): string {

    const PREFIXES = `
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
        @prefix sds: <https://w3id.org/sds#>.
        @prefix ex:  <https://example.org/ns#>.
        @prefix dct: <http://purl.org/dc/terms/>.
        @prefix ldes: <https://w3id.org/ldes#>.
    `;

    let record;

    if (!props.memberIsGraph) {
        record = `
            ${PREFIXES}

            ${props.withMetadata ? `sds:DataDescription {
                [] sds:stream ex:sdsStream;
                    sds:payload <https://example.org/entity/Entity_${props.memberIndex || 0}>.
            }` : ""}

            <https://example.org/entity/Entity_${props.memberIndex || 0}> a ex:Entity;
            ${props.changeType ? `ex:changeType <${props.changeType}>;` : ""}
            ${props.isPartOfTransaction ? `ldes:transactionId "transact_123";` : ""}
            ${props.isLastOfTransaction ? `ldes:isLastOfTransaction "true"^^xsd:boolean;` : ""}
            ${props.includeBlankNodes ? `ex:propBn _:bn_a;` : ""}
            ${props.includeAllProps ? `
                ex:prop1 "some value";
                ex:prop2 [
                    a ex:NestedEntity;
                    ex:nestedProp "some other value"
                ];
                ex:prop3 ex:SomeNamedNode;
                ex:propNum +30.
            ` : "."}
            ${props.includeBlankNodes ? `
            _:bn_a ex:bnProp _:bn_b.
            _:bn_b ex:bnProp2 "some bn value".
            ` : ""}
        `;
    } else {
        record = `
            ${PREFIXES}

            ${props.withMetadata ? `sds:DataDescription {
                [] sds:stream ex:sdsStream;
                    sds:payload <https://example.org/namedGraphs/Graph_${props.memberIndex || 0}>.
            }` : ""}
            

            <https://example.org/namedGraphs/Graph_${props.memberIndex || 0}> ${props.changeType ? `ex:changeType <${props.changeType}>;` : ""}
            ${props.isPartOfTransaction ? `<https://example.org/namedGraphs/Graph_${props.memberIndex || 0}> ldes:transactionId "transact_123";` : ""}
            ${props.isLastOfTransaction ? `<https://example.org/namedGraphs/Graph_${props.memberIndex || 0}> ldes:isLastOfTransaction "true"^^xsd:boolean;` : ""}
            <https://example.org/namedGraphs/Graph_${props.memberIndex || 0}> {
                <https://example.org/entity/Entity_A> a ex:Entity;
                    ${props.includeAllProps ? `
                        ex:prop1 "some value";
                        ex:prop2 [
                            a ex:NestedEntity;
                            ex:nestedProp "some other value"
                        ];
                        ex:prop3 ex:SomeNamedNode.
                    ` : "."}

                <https://example.org/entity/Entity_B> a ex:Entity;
                ${props.includeAllProps ? `
                    ex:prop1 "some value";
                    ex:prop2 [
                        a ex:NestedEntity;
                        ex:nestedProp "some other value"
                    ];
                    ex:prop3 ex:SomeNamedNode.
            ` : "."}
            }
        `;
    }
    return record;
}
