import { describe, expect, test } from "vitest";
import { OperationMode, SPARQLIngest } from "../src/SPARQLIngest";
import { ProcHelper, channel, createRunner } from "@rdfc/js-runner/lib/testUtils";
import { mkdir, readFile, rm } from "fs/promises";
import { resolve } from "path";
import { fromPath } from "@rdfc/utils-js";

import type { FullProc } from "@rdfc/js-runner";

describe("Tests for SPARQL ingest processor", async () => {
    test("rdfcp:SPARQLIngest is properly defined", async () => {
        const processor = `
            @prefix rdfc: <https://w3id.org/rdf-connect#>.
            @prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.

            <http://example.com/ns#processor> a rdfcp:SPARQLIngest; 
                rdfcp:input <inputChannel>;
                rdfcp:ingestConfig [
                    rdfcp:operationMode "Replication";
                    rdfcp:memberBatchSize 100;
                    rdfcp:memberShape "Some SHACL shape";
                    rdfcp:changeSemantics [
                        rdfcp:changeTypePath "http://ex.org/changeProp";
                        rdfcp:createValue "http://ex.org/Create";
                        rdfcp:updateValue "http://ex.org/Update";
                        rdfcp:deleteValue "http://ex.org/Delete"
                    ];
                    rdfcp:targetNamedGraph "http://ex.org/myGraph";
                    rdfcp:transactionConfig [
                        rdfcp:transactionIdPath "http://ex.org/transactionId";
                        rdfcp:transactionEndPath "http://ex.org/transactionEnd"
                    ];
                    rdfcp:graphStoreUrl "http://ex.org/myGraphStore";
                    rdfcp:forVirtuoso true;
                    rdfcp:accessToken "someAccessToken";
                    rdfcp:measurePerformance [
                        rdfcp:name "PerformanceTest";
                        rdfcp:outputPath "/some/output/path";
                        rdfcp:failureIsFatal true;
                        rdfcp:queryTimeout 30
                    ]
                ];
                rdfcp:output <outputChannel>.
        `;

        const configLocation = process.cwd() + "/processors.ttl";

        const procHelper = new ProcHelper<FullProc<SPARQLIngest>>();
        // Load processor semantic definition
        await procHelper.importFile(configLocation);
        // Load processor instance declaration
        await procHelper.importInline("pipeline.ttl", processor);

        // Get processor configuration
        procHelper.getConfig("SPARQLIngest");

        // Instantiate processor from declared instance
        const proc: FullProc<SPARQLIngest> = await procHelper.getProcessor("http://example.com/ns#processor");

        expect(proc).toBeDefined();

        expect(proc.memberStream.uri).toContain("inputChannel");
        expect(proc.sparqlWriter!.uri).toContain("outputChannel");

        expect(proc.config.operationMode).toBe(OperationMode.REPLICATION);
        expect(proc.config.memberBatchSize).toBe(100);
        expect(proc.config.memberShape).toBe("Some SHACL shape");
        expect(proc.config.changeSemantics!.changeTypePath).toBe("http://ex.org/changeProp");
        expect(proc.config.changeSemantics!.createValue).toBe("http://ex.org/Create");
        expect(proc.config.changeSemantics!.updateValue).toBe("http://ex.org/Update");
        expect(proc.config.changeSemantics!.deleteValue).toBe("http://ex.org/Delete");
        expect(proc.config.targetNamedGraph).toBe("http://ex.org/myGraph");
        expect(proc.config.transactionConfig!.transactionIdPath).toBe("http://ex.org/transactionId");
        expect(proc.config.transactionConfig!.transactionEndPath).toBe("http://ex.org/transactionEnd");
        expect(proc.config.graphStoreUrl).toBe("http://ex.org/myGraphStore");
        expect(proc.config.forVirtuoso).toBeTruthy();
        expect(proc.config.accessToken).toBe("someAccessToken");
        expect(proc.config.measurePerformance!.name).toBe("PerformanceTest");
        expect(proc.config.measurePerformance!.outputPath).toBe("/some/output/path");
        expect(proc.config.measurePerformance!.failureIsFatal).toBeTruthy();
        expect(proc.config.measurePerformance!.queryTimeout).toBe(30);
    });

    test("rdfcp:SPARQLIngest falls back to its default values", async () => {
        const processor = `
            @prefix rdfc: <https://w3id.org/rdf-connect#>.
            @prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.

            <http://example.com/ns#processor> a rdfcp:SPARQLIngest;
                rdfcp:input <inputChannel>;
                rdfcp:ingestConfig [
                    rdfcp:measurePerformance [
                        rdfcp:name "PerformanceTest";
                        rdfcp:outputPath "/some/output/path"
                    ]
                ].
        `;

        const configLocation = process.cwd() + "/processors.ttl";

        const procHelper = new ProcHelper<FullProc<SPARQLIngest>>();
        await procHelper.importFile(configLocation);
        await procHelper.importInline("pipeline.ttl", processor);
        procHelper.getConfig("SPARQLIngest");

        const proc: FullProc<SPARQLIngest> = await procHelper.getProcessor("http://example.com/ns#processor");

        expect(proc.sparqlWriter).toBeUndefined();
        expect(proc.config.operationMode).toBe(OperationMode.SYNC);
        expect(proc.config.memberBatchSize).toBe(100);
        expect(proc.config.forVirtuoso).toBe(false);
        expect(proc.config.measurePerformance!.failureIsFatal).toBe(false);
        expect(proc.config.measurePerformance!.queryTimeout).toBe(1800);
    });

    test("URLs and the output path can be written as IRIs", async () => {
        const processor = `
            @prefix rdfc: <https://w3id.org/rdf-connect#>.
            @prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.

            <http://example.com/ns#processor> a rdfcp:SPARQLIngest;
                rdfcp:input <inputChannel>;
                rdfcp:ingestConfig [
                    rdfcp:graphStoreUrl <http://ex.org/myGraphStore>;
                    rdfcp:measurePerformance [
                        rdfcp:name "PerformanceTest";
                        rdfcp:outputPath <./tests/perf-out>
                    ]
                ];
                rdfcp:output <outputChannel>.
        `;

        const procHelper = new ProcHelper<FullProc<SPARQLIngest>>();
        await procHelper.importFile(process.cwd() + "/processors.ttl");
        await procHelper.importInline(resolve("pipeline.ttl"), processor);
        procHelper.getConfig("SPARQLIngest");

        const proc: FullProc<SPARQLIngest> = await procHelper.getProcessor("http://example.com/ns#processor");

        expect(proc.config.graphStoreUrl).toBe("http://ex.org/myGraphStore");
        expect(proc.config.measurePerformance!.outputPath).toBe(
            fromPath("./tests/perf-out"),
        );

        // The performance log is written to the folder the IRI points to
        const outDir = resolve("./tests/perf-out");
        await mkdir(outDir, { recursive: true });
        try {
            const runner = createRunner();
            const [memberStreamWriter, memberStream] = channel(runner, "members");
            const ingest = <FullProc<SPARQLIngest>>new SPARQLIngest({
                memberStream,
                config: proc.config,
            }, proc.logger);
            await ingest.init();
            const processing = ingest.transform();
            await memberStreamWriter.close();
            await processing;

            const log = await readFile(resolve(outDir, "PerformanceTest.json"), "utf-8");
            expect(JSON.parse(log)).toEqual([]);
        } finally {
            await rm(outDir, { recursive: true, force: true });
        }
    });
});
