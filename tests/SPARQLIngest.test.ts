import { describe, test, expect } from "vitest";
import { SimpleStream } from "@rdfc/js-runner";
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";
import { Writer as N3Writer, Parser } from "n3";
import { QueryEngine } from "@comunica/query-sparql";
import { sparqlIngest } from "../src/SPARQLIngest";
import { SDS } from "@treecg/types";

import type { Bindings } from "@rdfjs/types";
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

        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
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

                stream.on("data", (bindings: Bindings) => {
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
        await memberStream.push(dataGenerator({
            changeType: config.changeSemantics!.createValue,
            includeAllProps: true,
            withMetadata: true,
        }));

        await ingestPromise;
    });

    test("Default SDS Member DELETE/INSERT into a populated SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false
        };

        // Add some data to the triple store first
        const localStore = RdfStore.createDefault();
        new Parser().parse(dataGenerator({ includeAllProps: true })).forEach(quad => localStore.addQuad(quad));
        const myEngine = new QueryEngine();

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
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

                stream.on("data", (bindings: Bindings) => {
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

        await memberStream.push(new N3Writer().quadsToString(store.getQuads()));

        await ingestPromise;
    });

    test("Default SDS Member DELETE/INSERT into an empty SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false
        };

        const localStore = RdfStore.createDefault();
        const myEngine = new QueryEngine();

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
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

                stream.on("data", (bindings: Bindings) => {
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

        await memberStream.push(new N3Writer().quadsToString(store.getQuads()));

        await ingestPromise;
    });

    test("SDS Member DELETE without shape and including all properties in a SPARQL endpoint", async () => {
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
        const localStore = RdfStore.createDefault();
        new Parser().parse(dataGenerator({ includeAllProps: true })).forEach(quad => localStore.addQuad(quad));
        const myEngine = new QueryEngine();

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await myEngine.queryVoid(query, {
                    sources: [localStore],
                });

                // Query the triple store to verify that triples were deleted properly
                const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                    sources: [localStore],
                });

                let counter = 0;
                stream.on("data", (bindings: Bindings) => {
                    const s = bindings.get("s");
                    const p = bindings.get("p");
                    const o = bindings.get("o");
                    console.log(s?.value, p?.value, o?.value);
                    counter++;
                }).on("end", () => {
                    // No triples should be left in the store
                    expect(counter).toBe(0);
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

        await memberStream.push(dataGenerator({
            changeType: config.changeSemantics!.deleteValue,
            includeAllProps: true,
            withMetadata: true,
        }));

        await ingestPromise;
    });

    test("SDS Member DELETE with declared shape and type in a SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false,
            memberShapes: [
                ENTITY_SHAPE,
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
        const localStore = RdfStore.createDefault();
        new Parser().parse(dataGenerator({ includeAllProps: true })).forEach(quad => localStore.addQuad(quad));
        const myEngine = new QueryEngine();

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await myEngine.queryVoid(query, {
                    sources: [localStore],
                });

                // Query the triple store to verify that triples were deleted properly
                const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                    sources: [localStore],
                });

                let counter = 0;
                stream.on("data", () => {
                    counter++;
                }).on("end", () => {
                    expect(counter).toBe(0);
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

        // Prepare property-less (only type and change type) member
        await memberStream.push(dataGenerator({
            changeType: config.changeSemantics!.deleteValue,
            includeAllProps: false,
            withMetadata: true,
        }));

        await ingestPromise;
    });

    test("SDS Member DELETE with declared shape and no type in a SPARQL endpoint", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false,
            memberShapes: [
                ENTITY_SHAPE,
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

        const localStore = RdfStore.createDefault();
        new Parser().parse(dataGenerator({ includeAllProps: true })).forEach(quad => localStore.addQuad(quad));
        const myEngine = new QueryEngine();

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await myEngine.queryVoid(query, {
                    sources: [localStore],
                });

                // Query the triple store to verify that triples were deleted properly
                const stream = await myEngine.queryBindings("SELECT * WHERE { ?s ?p ?o }", {
                    sources: [localStore],
                });

                let counter = 0;
                stream.on("data", () => {
                    counter++;
                }).on("end", () => {
                    expect(counter).toBe(0);
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

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

        await memberStream.push(new N3Writer().quadsToString(store.getQuads()));

        await ingestPromise;
    });

    test("Transaction-aware SDS Member ingestion into a SPARQL endpoint (with shape description for deletes)", async () => {
        const memberStream = new SimpleStream<string>();
        const sparqlWriter = new SimpleStream<string>();
        const config: IngestConfig = {
            memberIsGraph: false,
            changeSemantics: {
                changeTypePath: "https://example.org/ns#changeType",
                createValue: "https://example.org/ns#Create",
                updateValue: "https://example.org/ns#Update",
                deleteValue: "https://example.org/ns#Delete",
            },
            memberShapes: [ENTITY_SHAPE],
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

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
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

                stream.on("data", (bindings: Bindings) => {
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

                }).on("end", () => {
                    expect(sawEntity0).toBeTruthy();
                    expect(sawEntity1).toBeTruthy();
                    expect(sawEntity2).toBeFalsy();
                    expect(sawEntity3).toBeFalsy();
                    expect(sawEntity4).toBeTruthy();
                    expect(sawEntity5).toBeTruthy();
                    expect(sawNewPropIn0).toBeTruthy();
                    expect(sawNewPropIn1).toBeTruthy();
                    resolve();
                });
            });
        });

        // Execute processor function
        await sparqlIngest(memberStream, config, sparqlWriter);

        // Create new members ex:Entity_4 and ex:Entity_5
        await memberStream.push(dataGenerator({
            changeType: config.changeSemantics!.createValue,
            memberIndex: "4",
            includeAllProps: true,
            withMetadata: true,
            isPartOfTransaction: true,
            isLastOfTransaction: false,
        }));
        await memberStream.push(dataGenerator({
            changeType: config.changeSemantics!.createValue,
            memberIndex: "5",
            includeAllProps: true,
            withMetadata: true,
            isPartOfTransaction: true,
            isLastOfTransaction: false,
        }));

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
        await memberStream.push(new N3Writer().quadsToString(updateStore0.getQuads()));

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
        await memberStream.push(new N3Writer().quadsToString(updateStore1.getQuads()));

        // Delete members ex:Entity_2 and ex:Entity_3 by giving only its member type and shape
        await memberStream.push(dataGenerator({
            changeType: config.changeSemantics!.deleteValue,
            memberIndex: "2",
            includeAllProps: false,
            withMetadata: true,
            isPartOfTransaction: true,
            isLastOfTransaction: false
        }));
        await memberStream.push(dataGenerator({
            changeType: config.changeSemantics!.deleteValue,
            memberIndex: "3",
            includeAllProps: false,
            withMetadata: true,
            isPartOfTransaction: true,
            isLastOfTransaction: true
        }));

        await ingestPromise;
    });
});

function dataGenerator(props: {
    changeType?: string,
    memberIndex?: string,
    withMetadata?: boolean,
    includeAllProps?: boolean,
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
                ${props.includeAllProps ? `
                    ex:prop1 "some value";
                    ex:prop2 [
                        a ex:NestedEntity;
                        ex:nestedProp "some other value"
                    ];
                    ex:prop3 ex:SomeNamedNode;
                    ex:propNum +30.
                ` : "."}
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