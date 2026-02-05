import { describe, expect, test } from "vitest";
import { OperationMode, SPARQLIngest } from "../src/SPARQLIngest";
import { ProcHelper } from "@rdfc/js-runner/lib/testUtils";

import type { FullProc } from "@rdfc/js-runner";

describe("Tests for SPARQL ingest processor", async () => {
    test("rdfc:SPARQLIngest is properly defined", async () => {
        const processor = `
            @prefix rdfc: <https://w3id.org/rdf-connect#>.
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.

            <http://example.com/ns#processor> a rdfc:SPARQLIngest; 
                rdfc:memberStream <inputChannel>;
                rdfc:ingestConfig [
                    rdfc:operationMode "Replication";
                    rdfc:memberBatchSize 100;
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

        const configLocation = process.cwd() + "/processors.ttl";

        const procHelper = new ProcHelper<FullProc<SPARQLIngest>>();
        // Load processor semantic definition
        await procHelper.importFile(configLocation);
        // Load processor instance declaration
        await procHelper.importInline("pipeline.ttl", processor);

        // Get processor configuration
        procHelper.getConfig("SPARQLIngest");

        console.log(procHelper.config);
        // Instantiate processor from declared instance
        const proc: FullProc<SPARQLIngest> = await procHelper.getProcessor("http://example.com/ns#processor");

        expect(proc).toBeDefined();

        expect(proc.memberStream.uri).toContain("inputChannel");
        expect(proc.sparqlWriter!.uri).toContain("outputChannel");

        expect(proc.config.operationMode).toBe(OperationMode.REPLICATION);
        expect(proc.config.memberBatchSize).toBe(100);
        expect(proc.config.memberIsGraph).toBeFalsy();
        expect(proc.config.memberShapes![0]).toBe("Some SHACL shape");
        expect(proc.config.memberShapes![1]).toBe("Another SHACL shape");
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
});