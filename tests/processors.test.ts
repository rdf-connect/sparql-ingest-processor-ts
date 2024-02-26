import { describe, expect, test } from "@jest/globals";
import { extractProcessors, extractSteps, Source } from "@ajuvercr/js-runner";
import { resolve } from "path";

describe("Tests for SPARQL ingest processor", async () => {
    const pipeline = `
        @prefix js: <https://w3id.org/conn/js#>.
        @prefix ws: <https://w3id.org/conn/ws#>.
        @prefix : <https://w3id.org/conn#>.
        @prefix owl: <http://www.w3.org/2002/07/owl#>.
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
        @prefix sh: <http://www.w3.org/ns/shacl#>.

        <> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./processors.ttl>.

        [ ] a :Channel;
            :reader <jr>;
            :writer <jw>.
        <jr> a js:JsReaderChannel.
        <jw> a js:JsWriterChannel.
    `;

    const baseIRI = process.cwd() + "/config.ttl";

    test("js:SPARQLIngest is properly defined", async () => {
        const proc = `
            [ ] a js:SPARQLIngest; 
                js:memberStream <jr>;
                js:ingestConfig [
                    js:memberIsGraph false;
                    js:memberShape "Some SHACL shape", "Another SHACL shape";
                    js:changeSemantics [
                        js:changeTypePath "http://ex.org/changeProp";
                        js:createValue "http://ex.org/Create";
                        js:updateValue "http://ex.org/Update";
                        js:deleteValue "http://ex.org/Delete"
                    ];
                    js:targetNamedGraph "http://ex.org/myGraph";
                    js:transactionIdPath "http://ex.org/trancationId"
                ];
                js:sparqlWriter <jw>.
        `;

        const source: Source = {
            value: pipeline + proc,
            baseIRI,
            type: "memory",
        };

        const { processors, quads, shapes: config } = await extractProcessors(source);

        const env = processors.find((x) => x.ty.value === "https://w3id.org/conn/js#SPARQLIngest")!;
        expect(env).toBeDefined();

        const argss = extractSteps(env, quads, config);
        expect(argss.length).toBe(1);
        expect(argss[0].length).toBe(3);

        const [[memberStream, ingestConfig, sparqlWriter]] = argss;
        
        testReader(memberStream);
        expect(ingestConfig.memberIsGraph).toBeFalsy();
        expect(ingestConfig.memberShapes[0]).toBe("Some SHACL shape");
        expect(ingestConfig.memberShapes[1]).toBe("Another SHACL shape");
        expect(ingestConfig.changeSemantics.changeTypePath).toBe("http://ex.org/changeProp");
        expect(ingestConfig.changeSemantics.createValue).toBe("http://ex.org/Create");
        expect(ingestConfig.changeSemantics.updateValue).toBe("http://ex.org/Update");
        expect(ingestConfig.changeSemantics.deleteValue).toBe("http://ex.org/Delete");
        expect(ingestConfig.targetNamedGraph).toBe("http://ex.org/myGraph");
        expect(ingestConfig.transactionIdPath).toBe("http://ex.org/trancationId");
        testWriter(sparqlWriter);

        await checkProc(env.file, env.func);
    });
});

function testReader(arg: any) {
    expect(arg).toBeInstanceOf(Object);
    expect(arg.channel).toBeDefined();
    expect(arg.channel.id).toBeDefined();
    expect(arg.ty).toBeDefined();
}

function testWriter(arg: any) {
    expect(arg).toBeInstanceOf(Object);
    expect(arg.channel).toBeDefined();
    expect(arg.channel.id).toBeDefined();
    expect(arg.ty).toBeDefined();
}

async function checkProc(location: string, func: string) {
    const mod = await import("file://" + location);
    expect(mod[func]).toBeDefined();
}