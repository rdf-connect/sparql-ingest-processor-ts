import { describe, expect, test } from "vitest";
import { Parser } from "n3";
//import { parse_processors } from "@rdfc/orchestrator-js";

describe("Tests for SPARQL ingest processor", async () => {
    const pipeline = `
        @prefix rdfc: <https://w3id.org/rdf-connect#>.
        @prefix owl: <http://www.w3.org/2002/07/owl#>.

        <> owl:imports <./node_modules/@rdfc/js-runner/index.ttl>, <./processors.ttl>.

        <inputChannel> a rdfc:Reader, rdfc:Writer.
        <outputChannel> a rdfc:Reader, rdfc:Writer.
    `;

    test("rdfc:SPARQLIngest is properly defined", async () => {
        const proc = `
            [ ] a rdfc:SPARQLIngest; 
                rdfc:memberStream <inputChannel>;
                rdfc:ingestConfig [
                    rdfc:memberIsGraph false;
                    rdfc:memberShape "Some SHACL shape", "Another SHACL shape";
                    rdfc:changeSemantics [
                        rdfc:changeTypePath "http://ex.org/changeProp";
                        rdfc:createValue "http://ex.org/Create";
                        rdfc:updateValue "http://ex.org/Update";
                        rdfc:deleteValue "http://ex.org/Delete"
                    ];
                    rdfc:targetNamedGraph "http://ex.org/myGraph";
                    rdfc:transactionConfig [
                        rdfc:transactionIdPath "http://ex.org/transactionId";
                        rdfc:transactionEndPath "http://ex.org/transactionEnd"
                    ];
                    rdfc:graphStoreUrl "http://ex.org/myGraphStore";
                    rdfc:forVirtuoso true;
                    rdfc:accessToken "someAccessToken";
                    rdfc:measurePerformance [
                        rdfc:name "PerformanceTest";
                        rdfc:outputPath "/some/output/path";
                        rdfc:failureIsFatal true;
                        rdfc:queryTimeout 30
                    ]
                ];
                rdfc:sparqlWriter <outputChannel>.
        `;

        const source = pipeline + proc;
        const n3Parser = new Parser();
        const quads = n3Parser.parse(source);

        // TODO: FIX ME

        //const processors = await parse_processors(quads);
        //console.log(processors);

        // const { processors, quads, shapes: config } = await extractProcessors(source);

        // const env = processors.find((x) => x.ty.value === "https://w3id.org/conn/js#SPARQLIngest")!;
        // expect(env).toBeDefined();

        // const argss = extractSteps(env, quads, config);
        // expect(argss.length).toBe(1);
        // expect(argss[0].length).toBe(3);

        // const [[memberStream, ingestConfig, sparqlWriter, transactionConfig]] = argss;

        // testReader(memberStream);
        // expect(ingestConfig.memberIsGraph).toBeFalsy();
        // expect(ingestConfig.memberShapes[0]).toBe("Some SHACL shape");
        // expect(ingestConfig.memberShapes[1]).toBe("Another SHACL shape");
        // expect(ingestConfig.changeSemantics.changeTypePath).toBe("http://ex.org/changeProp");
        // expect(ingestConfig.changeSemantics.createValue).toBe("http://ex.org/Create");
        // expect(ingestConfig.changeSemantics.updateValue).toBe("http://ex.org/Update");
        // expect(ingestConfig.changeSemantics.deleteValue).toBe("http://ex.org/Delete");
        // expect(ingestConfig.targetNamedGraph).toBe("http://ex.org/myGraph");
        // expect(ingestConfig.transactionConfig.transactionIdPath).toBe("http://ex.org/transactionId");
        // expect(ingestConfig.transactionConfig.transactionEndPath).toBe("http://ex.org/transactionEnd");
        // expect(ingestConfig.graphStoreUrl).toBe("http://ex.org/myGraphStore");
        // expect(ingestConfig.forVirtuoso).toBeTruthy();
        // expect(ingestConfig.accessToken).toBe("someAccessToken");
        // expect(ingestConfig.measurePerformance.name).toBe("PerformanceTest");
        // expect(ingestConfig.measurePerformance.outputPath).toBe("/some/output/path");
        // expect(ingestConfig.measurePerformance.failureIsFatal).toBeTruthy();
        // expect(ingestConfig.measurePerformance.queryTimeout).toBe(30);
        // testWriter(sparqlWriter);

        // await checkProc(env.file, env.func);
    });
});

function testReader(arg: any) {
    expect(arg).toBeInstanceOf(Object);
    expect(arg.ty).toBeDefined();
    expect(arg.config.channel).toBeDefined();
    expect(arg.config.channel.id).toBeDefined();
}

function testWriter(arg: any) {
    expect(arg).toBeInstanceOf(Object);
    expect(arg.ty).toBeDefined();
    expect(arg.config.channel).toBeDefined();
    expect(arg.config.channel.id).toBeDefined();
}

async function checkProc(location: string, func: string) {
    const mod = await import("file://" + location);
    expect(mod[func]).toBeDefined();
}