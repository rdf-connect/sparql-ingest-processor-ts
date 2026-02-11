import { Quad_Object } from "@rdfjs/types";
import { RDF, SHACL } from "@treecg/types";
import { Writer as N3Writer, Parser } from "n3"
import { RdfStore } from "rdf-stores";
import { DataFactory } from "rdf-data-factory";
import {
    getObjects,
    getSubjects,
    splitStoreOnSize,
    splitStorePerNamedGraph
} from "./Utils";

const df = new DataFactory();

export const CREATE = (
    store: RdfStore,
    forVirtuoso?: boolean,
): string[] => {
    const queries: string[] = [];

    // First we split the store into multiple sub-stores per named graph
    const storesPerGraph = splitStorePerNamedGraph(store);


    for (const { graph, store } of storesPerGraph) {
        // Split every named graph-based store into multiple sub-stores to avoid query length limits 
        // such as the 10000 SQL code lines in Virtuoso for large inserts. 
        // (see https://github.com/openlink/virtuoso-opensource/blob/develop/7/libsrc/Wi/sparql2sql.h#L1031).
        // 500 is an empirically obtained value to avoid exceeding the 10000 lines limit in Virtuoso.
        // 50000 is a large value for non-Virtuoso triple stores that usually benefit from larger queries.
        const subStores = splitStoreOnSize(store, forVirtuoso ? 500 : 50000);


        subStores.forEach((s, i) => {
            queries.push(`
                INSERT DATA {
                    ${graph.equals(df.defaultGraph()) ? "" : `GRAPH <${graph.value}> {`}
                        ${new N3Writer().quadsToString(s.getQuads().map(q => {
                return df.quad(q.subject, q.predicate, q.object, df.defaultGraph());
            }))}
                    ${graph.equals(df.defaultGraph()) ? "" : `}`}
                }
            `);
        });
    }

    return queries;
};

// We have to use a multiple query request of a DELETE WHERE + INSERT DATA for the default update operation
// because some triple stores like Virtuoso fail on executing a DELETE INSERT WHERE when there is no data to delete.
export const UPDATE = (
    store: RdfStore,
    forVirtuoso?: boolean,
): string[] => {
    const queries: string[] = [];

    // First we split the store into multiple sub-stores per named graph
    const storesPerGraph = splitStorePerNamedGraph(store);

    for (const { graph, store } of storesPerGraph) {
        // Split every store into multiple sub-stores to avoid query length limits 
        // such as the 10000 SQL code lines in Virtuoso for large inserts. 
        // (see https://github.com/openlink/virtuoso-opensource/blob/develop/7/libsrc/Wi/sparql2sql.h#L1031).
        // 500 is an empirically obtained value to avoid exceeding the 10000 lines limit in Virtuoso.
        // 50000 is a large value for non-Virtuoso triple stores that usually benefit from larger queries.
        const subStores = splitStoreOnSize(store, forVirtuoso ? 500 : 50000);

        const formattedQuery = formatQuery(store);

        const deleteInsertQuery = [`
            ${graph.equals(df.defaultGraph()) ? "" : `WITH <${graph.value}>`}
            DELETE { 
                ${formattedQuery[0]} 
            }
            WHERE { 
                ${formattedQuery[0]} 
            }
        `];

        subStores.forEach((s, i) => {
            deleteInsertQuery.push(`
                INSERT DATA {
                    ${graph.equals(df.defaultGraph()) ? "" : `GRAPH <${graph.value}> {`}
                        ${new N3Writer().quadsToString(s.getQuads().map(q => {
                return df.quad(q.subject, q.predicate, q.object, df.defaultGraph());
            }))}
                    ${graph.equals(df.defaultGraph()) ? "" : `}`}
                }
            `);
        });

        queries.push(...deleteInsertQuery);
    }

    return queries;
};

export const DELETE = (
    store: RdfStore,
    memberIRI: string,
    memberShape?: string,
): string[] => {
    const queries: string[] = [];

    // First we split the store into multiple sub-stores per named graph
    const storesPerGraph = splitStorePerNamedGraph(store);

    for (const { graph, store } of storesPerGraph) {
        const formatted = formatQuery(store, memberIRI, memberShape);
        const deleteBuilder = formatted.length > 1 ? formatted[1] : formatted[0];
        const whereBuilder = formatted[0];

        queries.push(`
            ${graph.equals(df.defaultGraph()) ? "" : `WITH <${graph.value}>`}
            DELETE {
                ${deleteBuilder}
            } WHERE {
                ${whereBuilder}
            }
        `);
    }

    return queries;
}

function formatQuery(
    memberStore: RdfStore,
    memberIRI?: string,
    memberShape?: string,
    indexStart: number = 0
): string[] {
    const subjectSet = new Set<string>();
    const blankNodeMap = new Map<string, string>();
    const queryBuilder: string[] = [];
    let i = indexStart;

    // Check if member shape was given. 
    // If not, we assume that all properties of the member are present 
    // and we use them to define its DELETE query pattern.
    if (!memberShape) {
        // Iterate over every BGP of the member to define a deletion pattern
        for (const quad of memberStore.getQuads()) {
            if (!subjectSet.has(quad.subject.value)) {
                // Make sure every subject is processed only once
                subjectSet.add(quad.subject.value);
                if (quad.subject.termType === "NamedNode") {
                    // Define a pattern that covers every property and value of this named node
                    queryBuilder.push(`<${quad.subject.value}> ?p_${i} ?o_${i}.`);
                } else if (quad.subject.termType === "BlankNode") {
                    if (!blankNodeMap.has(quad.subject.value)) {
                        // If the blank node is not yet mapped, create a new variable for it
                        blankNodeMap.set(quad.subject.value, `?bn_${i}`);
                    }
                    if (quad.object.termType === "BlankNode") {
                        // Create a variable for the object if it is a blank node and is the first time we see it
                        blankNodeMap.set(quad.object.value, `?bn_ref_${i}`);
                    }

                    // Define a pattern that covers the referencing BGP and every property and value of blank nodes
                    queryBuilder.push(`${blankNodeMap.get(quad.subject.value)} <${quad.predicate.value}> ${quad.object.termType === "Literal"
                        ? `"${quad.object.value}"^^<${quad.object.datatype.value}>`
                        : quad.object.termType === "BlankNode" ? `${blankNodeMap.get(quad.object.value)} `
                            : `<${quad.object.value}>`
                        }.`);
                    // Generic pattern to cover all properties of the blank node
                    queryBuilder.push(`${blankNodeMap.get(quad.subject.value)} ?p_${i} ?o_${i}.`);
                    // Generic pattern to cover all triples that reference the blank node
                    queryBuilder.push(`?s_ref_${i} ?p_ref_${i} ${blankNodeMap.get(quad.subject.value)}.`);
                }
                i++;
            }
        }
        return [queryBuilder.join("\n")];
    } else {
        const shapeStore = RdfStore.createDefault();
        new Parser().parse(memberShape).forEach(quad => shapeStore.addQuad(quad));

        // Add basic DELETE query pattern for this member
        queryBuilder.push(`<${memberIRI}> ?p_${i} ?o_${i}.`);

        // We have to define a different but related query pattern for the delete clause without OPTIONAL
        const deleteQueryBuilder: string[] = [];
        deleteQueryBuilder.push(`<${memberIRI}> ?p_${i} ?o_${i}.`);
        i++;

        const propShapes = getObjects(shapeStore, null, SHACL.terms.property, null);
        queryBuilder.push(" OPTIONAL { ");
        for (const propSh of propShapes) {
            const pred = getObjects(shapeStore, propSh, SHACL.terms.path, null)[0];
            queryBuilder.push(`<${memberIRI}> <${pred.value}> ?subEnt_${i}.`);
            deleteQueryBuilder.push(`<${memberIRI}> <${pred.value}> ?subEnt_${i}.`);
            queryBuilder.push(`?subEnt_${i} ?p_${i} ?o_${i}.`);
            deleteQueryBuilder.push(`?subEnt_${i} ?p_${i} ?o_${i}.`);
            i++;
        }
        queryBuilder.push(" }");

        return [queryBuilder.join("\n"), deleteQueryBuilder.join("\n")];
    }
}