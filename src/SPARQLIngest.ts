import type { Stream, Writer } from "@ajuvercr/js-runner";
import { SDS } from "@treecg/types";
import { Store, Parser } from "n3";
import { CREATE, UPDATE, DELETE } from "./SPARQLQueries";

export type ChangeSemantics = {
    changeTypePath: string;
    createValue: string;
    updateValue: string;
    deleteValue: string;
};

export type IngestConfig = {
    memberIsGraph: boolean;
    memberShapes?: string[];
    changeSemantics?: ChangeSemantics;
    targetNamedGraph?: string;
    transactionIdPath?: string;
};

export async function sparqlIngest(
    memberStream: Stream<string>,
    config: IngestConfig,
    sparqlWriter: Writer<string>
) {
    memberStream.data(async rawQuads => {
        // TODO handle transaction-based bulk writes
        const quads = new Parser().parse(rawQuads);
        const store = new Store(quads);

        // Get member IRI form SDS description
        const memberIRI = store.getObjects(null, SDS.payload, null)[0];

        if (memberIRI) {
            // Remove SDS wrapper
            store.removeQuads(store.getQuads(null, SDS.stream, null, null));
            store.removeQuads(store.getQuads(null, SDS.payload, null, null));

            // Determine if we have a named graph (either explicitly configure or as the member itself)
            let ng = undefined;
            if (config.memberIsGraph) {
                ng = memberIRI.value;
            } else if (config.targetNamedGraph) {
                ng = config.targetNamedGraph;
            }

            let query;

            if (config.changeSemantics) {
                // Extract and remove change type value
                const ctv = store.getQuads(null, config.changeSemantics.changeTypePath, null, null)[0];
                if (ctv) {
                    store.removeQuad(ctv);
                }

                if (ctv.object.value === config.changeSemantics.createValue) {
                    query = CREATE(store, ng);
                } else if (ctv.object.value === config.changeSemantics.updateValue) {
                    query = UPDATE(store, ng);
                } else if (ctv.object.value === config.changeSemantics.deleteValue) {
                    query = DELETE(store, memberIRI.value, config.memberShapes, ng);
                } else {
                    throw new Error(`Unrecognized change type value: ${ctv.object.value}`);
                }
            } else {
                // If no change semantics are provided we do a DELETE/INSERT query by default
                query = UPDATE(store, ng);
            }

            // Execute the update query
            await sparqlWriter.push(query);
        } else {
            throw new Error(`No member IRI found in received RDF data: \n${rawQuads}`);
        }
    });

    memberStream.on("end", async () => await sparqlWriter.end());
}