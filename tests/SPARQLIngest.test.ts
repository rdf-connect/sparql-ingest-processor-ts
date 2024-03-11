import { describe, test, expect, beforeAll, afterEach } from "@jest/globals";
import { SimpleStream } from "@ajuvercr/js-runner";
import { DataFactory as DF, Writer as N3Writer, Parser, Store } from "n3";
import { MemoryLevel } from "memory-level";
import { Quadstore } from "quadstore";
import { Engine } from "quadstore-comunica";
import { sparqlIngest, IngestConfig } from "../src/SPARQLIngest";
import { Bindings } from "@rdfjs/types";
import { SDS } from "@treecg/types";

describe("Functional tests for the sparqlIngest Connector Architecture function", () => {

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
        await memberStream.push(dataGenerator(config.changeSemantics!.createValue, "0", true));

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
        const store = new Store(new Parser().parse(dataGenerator()));
        store.addQuad(
            DF.namedNode("https://example.org/entity/Entity_0"),
            DF.namedNode("https://example.org/ns#newProp"),
            DF.literal("Updated value")
        );

        await memberStream.push(new N3Writer().quadsToString(store.getQuads(null, null, null, null)));

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
        await memberStream.push(dataGenerator(config.changeSemantics!.deleteValue, "0", false));

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
                DF.namedNode("https://example.org/entity/Entity_0")
            ),
            DF.quad(
                DF.namedNode("https://example.org/entity/Entity_0"),
                DF.namedNode("https://example.org/ns#changeType"),
                DF.namedNode("https://example.org/ns#Delete")
            )
        ]);

        await memberStream.push(new N3Writer().quadsToString(store.getQuads(null, null, null, null)));

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
        await Promise.all([
            quadstore.multiPut(new Parser().parse(dataGenerator(undefined, "0", true))),
            quadstore.multiPut(new Parser().parse(dataGenerator(undefined, "1", true))),
            quadstore.multiPut(new Parser().parse(dataGenerator(undefined, "2", true))),
            quadstore.multiPut(new Parser().parse(dataGenerator(undefined, "3", true)))
        ]);

        const ingestPromise = new Promise<void>(resolve => {
            sparqlWriter.data(async query => {
                // Execute produced SPARQL query
                await engine.queryVoid(query);

                /**
                 * Whe should see that:
                 * - ex:Entity_0 and ex:Entity_1 were updated
                 * - ex:Entity_4 and ex:Entity_5 were created
                 * - ex:Entity_2 and ex:Entity_3 were deleted
                 */
                // Query the triple store to verify that triples were updated properly
                const stream = await engine.queryBindings("SELECT * WHERE { ?s ?p ?o }");
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
        await memberStream.push(dataGenerator(
            config.changeSemantics!.createValue,
            "4",
            true,
            true, // is part of transaction
            false // is last of transaction
        ));
        await memberStream.push(dataGenerator(
            config.changeSemantics!.createValue,
            "5",
            true,
            true, // is part of transaction
            false // is last of transaction
        ));

        // Update members ex:Entity_0 and ex:Entity_1
        const updateStore0 = new Store(new Parser().parse(dataGenerator(
            config.changeSemantics!.updateValue,
            "0",
            true,
            true, // is part of transaction
            false // is last of transaction
        )));
        updateStore0.addQuad(
            DF.namedNode("https://example.org/entity/Entity_0"),
            DF.namedNode("https://example.org/ns#newProp"),
            DF.literal("Updated value")
        );
        await memberStream.push(new N3Writer().quadsToString(updateStore0.getQuads(null, null, null, null)));
        
        const updateStore1 = new Store(new Parser().parse(dataGenerator(
            config.changeSemantics!.updateValue,
            "1",
            true,
            true, // is part of transaction
            false // is last of transaction
        )));
        updateStore1.addQuad(
            DF.namedNode("https://example.org/entity/Entity_1"),
            DF.namedNode("https://example.org/ns#newProp"),
            DF.literal("Updated value")
        );
        await memberStream.push(new N3Writer().quadsToString(updateStore1.getQuads(null, null, null, null)));

        // Delete members ex:Entity_2 and ex:Entity_3 by giving only its member type and shape
        await memberStream.push(dataGenerator(
            config.changeSemantics!.deleteValue,
            "2",
            false, // include all properties
            true, // is part of transaction
            false // is last of transaction
        ));
        await memberStream.push(dataGenerator(
            config.changeSemantics!.deleteValue,
            "3",
            false, // include all properties
            true, // is part of transaction
            true // is last of transaction
        ));

        await ingestPromise;
    });
});

function dataGenerator(
    changeType?: string,
    memberIndex?: string,
    includeAllProps: boolean = true,
    isPartOfTransaction: boolean = false,
    isLastOfTransaction: boolean = false,
    memberIsGraph: boolean = false,
): string {

    const PREFIXES = `
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
        @prefix sds: <https://w3id.org/sds#>.
        @prefix ex:  <https://example.org/ns#>.
        @prefix dct: <http://purl.org/dc/terms/>.
        @prefix ldes: <https://w3id.org/ldes#>.
    `;

    let record;

    if (!memberIsGraph) {
        record = `
            ${PREFIXES}

            [] sds:stream ex:sdsStream;
                sds:payload <https://example.org/entity/Entity_${memberIndex || 0}>.

            <https://example.org/entity/Entity_${memberIndex || 0}> a ex:Entity;
                ${changeType ? `ex:changeType <${changeType}>;` : ""}
                ${isPartOfTransaction ? `ldes:transactionId "transact_123";` : ""}
                ${isLastOfTransaction ? `ldes:isLastOfTransaction "true"^^xsd:boolean;` : ""}
                ${includeAllProps ? `
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

            [] sds:stream ex:sdsStream;
                sds:payload <https://example.org/namedGraphs/Graph_${memberIndex || 0}>.

            <https://example.org/namedGraphs/Graph_${memberIndex || 0}> ${changeType ? `ex:changeType <${changeType}>;` : ""}
            ${isPartOfTransaction ? `<https://example.org/namedGraphs/Graph_${memberIndex || 0}> ldes:transactionId "transact_123";` : ""}
            ${isLastOfTransaction ? `<https://example.org/namedGraphs/Graph_${memberIndex || 0}> ldes:isLastOfTransaction "true"^^xsd:boolean;` : ""}
            <https://example.org/namedGraphs/Graph_${memberIndex || 0}> {
                <https://example.org/entity/Entity_A> a ex:Entity;
                    ${includeAllProps ? `
                        ex:prop1 "some value";
                        ex:prop2 [
                            a ex:NestedEntity;
                            ex:nestedProp "some other value"
                        ];
                        ex:prop3 ex:SomeNamedNode.
                    ` : "."}

                <https://example.org/entity/Entity_B> a ex:Entity;
                ${includeAllProps ? `
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