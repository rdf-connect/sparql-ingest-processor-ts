import { describe, test, expect, beforeAll, afterEach } from "@jest/globals";
import { SimpleStream } from "@ajuvercr/js-runner";
import { DataFactory as DF, Writer as N3Writer, Parser, Store } from "n3";
import { MemoryLevel } from "memory-level";
import { Quadstore } from "quadstore";
import { Engine } from "quadstore-comunica";
import { sparqlIngest, IngestConfig } from "../src/SPARQLIngest";
import { Bindings } from "@rdfjs/types";
import { RDF, SDS } from "@treecg/types";

describe("Functional tests for the sparqlIngest Connector Architecture function", () => {

    let quadstore = new Quadstore({ backend: new MemoryLevel<string, string>(), dataFactory: DF });
    let engine = new Engine(quadstore);

    beforeAll(async () => {
        await quadstore.open();
    });

    afterEach(async () => {
        await quadstore.close();
        quadstore = new Quadstore({ backend: new MemoryLevel<string, string>(), dataFactory: DF });
        engine = new Engine(quadstore);
        await quadstore.open();
    });

    test("SDS Member INSERT into a SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false,
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            }
        };

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await engine.queryVoid(query);

                // Query the triple store to verify that triples were inserted
                const stream = await engine.queryBindings("SELECT * WHERE { ?s ?p ?o }");
                let sawEntity = false;
                let sawProp2 = false;
                let sawNestedType = false;

                stream.on("data", (bindings: Bindings) => {
                    const s = bindings.get("s");
                    const p = bindings.get("p");
                    const o = bindings.get("o");

                    if (s?.value === "https://example.org/entity/Entity") {
                        sawEntity = true;
                    }
                    if (p?.value === "https://example.org/ns#prop2") {
                        sawProp2 = true;
                    }
                    if (o?.value === "https://example.org/ns#NestedEntity") {
                        sawNestedType = true;
                    }
                }).on("end", () => {
                    expect(sawEntity).toBeTruthy();
                    expect(sawProp2).toBeTruthy();
                    expect(sawNestedType).toBeTruthy();
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

        // Push 1 members for ingestion
        await memberStream.push(dataGenerator(config.changeSemantics!.createValue, false));

        await ingestPromise;
    });

    test("Default SDS Member DELETE/INSERT into a SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false
        };

        // Add some data to the triple store first
        await quadstore.multiPut(new Parser().parse(dataGenerator()));

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await engine.queryVoid(query);

                // Query the triple store to verify that triples were updated properly
                const stream = await engine.queryBindings("SELECT * WHERE { ?s ?p ?o }");
                let sawEntity = false;
                let sawProp2 = false;
                let sawNestedType = false;
                let sawNewProp = false;

                stream.on("data", (bindings: Bindings) => {
                    const s = bindings.get("s");
                    const p = bindings.get("p");
                    const o = bindings.get("o");

                    if (s?.value === "https://example.org/entity/Entity") {
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
                }).on("end", () => {
                    expect(sawEntity).toBeTruthy();
                    expect(sawProp2).toBeTruthy();
                    expect(sawNewProp).toBeTruthy();
                    expect(sawNestedType).toBeTruthy();
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

        // Prepare updated member
        const store = new Store(new Parser().parse(dataGenerator()));
        store.addQuad(
            DF.namedNode("https://example.org/entity/Entity"),
            DF.namedNode("https://example.org/ns#newProp"),
            DF.literal("Updated value")
        );

        await memberStream.push(new N3Writer().quadsToString(store.getQuads(null, null, null, null)));

        await ingestPromise;
    });

    test("SDS Member DELETE without shape in a SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false,
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            }
        };

        // Add some data to the triple store first
        await quadstore.multiPut(new Parser().parse(dataGenerator()));

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await engine.queryVoid(query);

                // Query the triple store to verify that triples were deleted properly
                const stream = await engine.queryBindings("SELECT * WHERE { ?s ?p ?o }");

                let counter = 0;
                stream.on("data", () => {
                    counter++;
                }).on("end", () => {
                    // Only the SDS triples should be there
                    expect(counter).toBe(2);
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

        await memberStream.push(dataGenerator(config.changeSemantics!.deleteValue));

        await ingestPromise;
    });

    test("SDS Member DELETE with declared shape and type in a SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false,
            memberShapes: [
                `
                    @prefix sh: <http://www.w3.org/ns/shacl#>.
                    @prefix ex: <https://example.org/ns#>.

                    [] a sh:NodeShape;
                      sh:targetClass ex:Entity;
                      sh:property [
                        sh:path ex:prop2;
                        sh:node [
                          a sh:NodeShape;
                          sh:targetClass ex:NestedEntity
                        ]
                      ].
                `,
                `
                    @prefix sh: <http://www.w3.org/ns/shacl#>.
                    @prefix ex: <https://example.org/ns#>.

                    [] a sh:NodeShape;
                      sh:targetClass ex:AnotherEntity.    
                `
            ],
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            }
        };

        // Add some initial data to the triple store
        await quadstore.multiPut(new Parser().parse(dataGenerator()));

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await engine.queryVoid(query);

                // Query the triple store to verify that triples were deleted properly
                const stream = await engine.queryBindings("SELECT * WHERE { ?s ?p ?o }");

                let counter = 0;
                stream.on("data", () => {
                    counter++;
                }).on("end", () => {
                    // Only the SDS triples should be there
                    expect(counter).toBe(2);
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

        // Prepare property-less (only type and change type) member
        const store = new Store();
        store.addQuads([
            DF.quad(
                DF.blankNode(),
                DF.namedNode(SDS.stream),
                DF.namedNode("https://example.org/ns#sdsStream")
            ),
            DF.quad(
                DF.blankNode(),
                DF.namedNode(SDS.payload),
                DF.namedNode("https://example.org/entity/Entity")
            ),
            DF.quad(
                DF.namedNode("https://example.org/entity/Entity"),
                DF.namedNode(RDF.type),
                DF.namedNode("https://example.org/ns#Entity")
            ),
            DF.quad(
                DF.namedNode("https://example.org/entity/Entity"),
                DF.namedNode("https://example.org/ns#changeType"),
                DF.namedNode("https://example.org/ns#Delete")
            )
        ]);

        await memberStream.push(new N3Writer().quadsToString(store.getQuads(null, null, null, null)));

        await ingestPromise;
    });

    test("SDS Member DELETE with declared shape and no type in a SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false,
            memberShapes: [
                `
                    @prefix sh: <http://www.w3.org/ns/shacl#>.
                    @prefix ex: <https://example.org/ns#>.

                    [] a sh:NodeShape;
                      sh:targetClass ex:Entity;
                      sh:property [
                        sh:path ex:prop2;
                        sh:node [
                          a sh:NodeShape;
                          sh:targetClass ex:NestedEntity
                        ]
                      ].
                `,
                `
                    @prefix sh: <http://www.w3.org/ns/shacl#>.
                    @prefix ex: <https://example.org/ns#>.

                    [] a sh:NodeShape;
                      sh:targetClass ex:AnotherEntity;
                      sh:property [
                        sh:path ex:otherProp;
                        sh:node [
                          a sh:NodeShape;
                          sh:targetClass ex:AnotherNestedEntity
                        ]
                      ].    
                `
            ],
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            }
        };

        // Add some initial data to the triple store
        await quadstore.multiPut(new Parser().parse(dataGenerator()));

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await engine.queryVoid(query);

                // Query the triple store to verify that triples were deleted properly
                const stream = await engine.queryBindings("SELECT * WHERE { ?s ?p ?o }");

                let counter = 0;
                stream.on("data", () => {
                    counter++;
                }).on("end", () => {
                    // Only the SDS triples should be there
                    expect(counter).toBe(2);
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

        // Prepare property-less (only type and change type) member
        const store = new Store();
        store.addQuads([
            DF.quad(
                DF.blankNode(),
                DF.namedNode(SDS.stream),
                DF.namedNode("https://example.org/ns#sdsStream")
            ),
            DF.quad(
                DF.blankNode(),
                DF.namedNode(SDS.payload),
                DF.namedNode("https://example.org/entity/Entity")
            ),
            DF.quad(
                DF.namedNode("https://example.org/entity/Entity"),
                DF.namedNode("https://example.org/ns#changeType"),
                DF.namedNode("https://example.org/ns#Delete")
            )
        ]);

        await memberStream.push(new N3Writer().quadsToString(store.getQuads(null, null, null, null)));

        await ingestPromise;
    });
});

function dataGenerator(changeType?: string, memberIsGraph: boolean = false): string {

    const PREFIXES = `
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
        @prefix sds: <https://w3id.org/sds#>.
        @prefix ex:  <https://example.org/ns#>.
        @prefix dct: <http://purl.org/dc/terms/>.
    `;

    let record;

    if (!memberIsGraph) {
        record = `
            ${PREFIXES}

            [] sds:stream ex:sdsStream;
                sds:payload <https://example.org/entity/Entity>.

            <https://example.org/entity/Entity> a ex:Entity;
                ${changeType ? `ex:changeType <${changeType}>;` : ""}
                ex:prop1 "some value";
                ex:prop2 [
                    a ex:NestedEntity;
                    ex:nestedProp "some other value"
                ];
                ex:prop3 ex:SomeNamedNode.
        `;
    } else {
        record = `
            ${PREFIXES}

            [] sds:stream ex:sdsStream;
                sds:payload <https://example.org/namedGraphs/Graph>.

            <https://example.org/namedGraphs/Graph> ${changeType ? `ex:changeType <${changeType}>;` : ""}
            <https://example.org/namedGraphs/Graph> {
                <https://example.org/entity/Entity_A> a ex:Entity;
                    ex:prop1 "some value";
                    ex:prop2 [
                        a ex:NestedEntity;
                        ex:nestedProp "some other value"
                    ];
                    ex:prop3 ex:SomeNamedNode.

                <https://example.org/entity/Entity_B> a ex:Entity;
                    ex:prop1 "some value";
                    ex:prop2 [
                        a ex:NestedEntity;
                        ex:nestedProp "some other value"
                    ];
                    ex:prop3 ex:SomeNamedNode.
            }
        `;
    }
    return record;
}