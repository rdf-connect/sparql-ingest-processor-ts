import { Quad_Object } from "@rdfjs/types";
import { RDF, SHACL } from "@treecg/types";
import { Writer as N3Writer, Parser } from "n3"
import { RdfStore } from "rdf-stores";
import { DataFactory } from "rdf-data-factory";
import { getObjects, getSubjects } from "./Utils";

const df = new DataFactory();

export const CREATE = (store: RdfStore, namedGraph?: string, multipleNamedGraphs?: boolean): string => {
    // TODO: Handle case of multiple members being Named Graphs
    const content = new N3Writer().quadsToString(store.getQuads(null, null, null, null));
    return `
        INSERT DATA {
            ${namedGraph ? `GRAPH <${namedGraph}> {${content}}` : `${content}`} 
        }
    `;
};

export const UPDATE = (store: RdfStore, namedGraph?: string, multipleNamedGraphs?: boolean): string => {
    // TODO: Handle case of multiple members being Named Graphs
    const formattedQuery = formatDeleteQuery(store);
    const content = new N3Writer().quadsToString(store.getQuads(null, null, null, null));
    return `
        ${namedGraph ? `WITH <${namedGraph}>` : ""}
        DELETE { 
            ${formattedQuery[0]} 
        }
        INSERT { 
            ${content} 
        }
        WHERE { 
            ${formattedQuery[0]} 
        }
    `;
};

export const DELETE = (
    store: RdfStore,
    memberIRIs: string[],
    memberShapes?: string[],
    namedGraph?: string,
    multipleNamedGraphs?: boolean
): string => {
    // TODO: Handle case of multiple members being Named Graphs
    const deleteBuilder = [];
    const whereBuilder = [];

    let indexStart = 0;
    for (const memberIRI of memberIRIs) {
        const formatted = formatDeleteQuery(store, memberIRI, memberShapes, indexStart);
        deleteBuilder.push(formatted.length > 1 ? formatted[1] : formatted[0]);
        whereBuilder.push(formatted[0]);
        indexStart++;
    }

    return `
        ${namedGraph ? `WITH <${namedGraph}>` : ""}
        DELETE {
            ${deleteBuilder.join("\n")}
        } WHERE {
            ${whereBuilder.join("\n")}
        }
    `;
}

function formatDeleteQuery(
    memberStore: RdfStore,
    memberIRI?: string,
    memberShapes?: string[],
    indexStart: number = 0
): string[] {
    const subjectSet = new Set<string>();
    const queryBuilder: string[] = [];
    const formattedQueries: string[] = [];
    let i = indexStart;

    // Check if one or more member shapes were given. 
    // If not, we assume that all properties of the member are present 
    // and we use them to define its DELETE query pattern.
    if (!memberShapes || memberShapes.length === 0) {
        // Iterate over every BGP of the member to define a deletion pattern
        for (const quad of memberStore.getQuads(null, null, null, null)) {
            if (!subjectSet.has(quad.subject.value)) {
                // Make sure every subject is processed only once
                subjectSet.add(quad.subject.value);
                if (quad.subject.termType === "NamedNode") {
                    // Define a pattern that covers every property and value of named nodes
                    queryBuilder.push(`<${quad.subject.value}> ?p_${i} ?o_${i}.`);
                } else if (quad.subject.termType === "BlankNode") {
                    // Define a pattern that covers the referencing BGP and every property and value of blank nodes
                    const bnQ = memberStore.getQuads(null, null, quad.subject, null)[0];
                    queryBuilder.push(`<${bnQ.subject.value}> <${bnQ.predicate.value}> ?bn_${i}.`);
                    queryBuilder.push(`?bn_${i} ?p_${i} ?o${i}.`);
                }
                i++;
            }
        }
        formattedQueries.push(queryBuilder.join("\n"))
    } else {
        // Create a shape index per target class
        const shapeIndex = new Map<string, RdfStore>();
        memberShapes.forEach(msh => {
            const shapeStore = RdfStore.createDefault();
            new Parser().parse(msh).forEach(quad => shapeStore.addQuad(quad));
            shapeIndex.set(extractMainTargetClass(shapeStore).value, shapeStore);
        });

        // Add basic DELETE query pattern for this member
        queryBuilder.push(`<${memberIRI}> ?p_${i} ?o_${i}.`);

        // See if the member has a defined rdf:type so that we create a query pattern 
        // for a specific shape, based on the sh:targetClass.
        // Otherwise we have to include all shapes in the query pattern because we don't know
        // exactly which is the shape of the received member.
        const memberType = getObjects(memberStore, df.namedNode(memberIRI!), RDF.terms.type, null)[0];
        if (memberType) {
            i++;
            const mshStore = shapeIndex.get(memberType.value);
            const propShapes = getObjects(mshStore!, null, SHACL.terms.property, null);

            for (const propSh of propShapes) {
                const pred = getObjects(mshStore!, propSh, SHACL.terms.path, null)[0];
                queryBuilder.push(`<${memberIRI}> <${pred.value}> ?subEnt_${i}.`);
                queryBuilder.push(`?subEnt_${i} ?p_${i} ?o_${i}.`);
                i++;
            }
            formattedQueries.push(queryBuilder.join("\n"));
        } else {
            // We have to define a different but related query pattern for the delete clause without OPTIONAL
            const deleteQueryBuilder: string[] = [];
            deleteQueryBuilder.push(`<${memberIRI}> ?p_${i} ?o_${i}.`);
            i++;

            // Iterate over every declared member shape
            shapeIndex.forEach(mshStore => {
                const propShapes = getObjects(mshStore, null, SHACL.terms.property, null);
                queryBuilder.push(" OPTIONAL { ");
                for (const propSh of propShapes) {
                    const pred = getObjects(mshStore, propSh, SHACL.terms.path, null)[0];
                    queryBuilder.push(`<${memberIRI}> <${pred.value}> ?subEnt_${i}.`);
                    deleteQueryBuilder.push(`<${memberIRI}> <${pred.value}> ?subEnt_${i}.`);
                    queryBuilder.push(`?subEnt_${i} ?p_${i} ?o_${i}.`);
                    deleteQueryBuilder.push(`?subEnt_${i} ?p_${i} ?o_${i}.`);
                    i++;
                }
                queryBuilder.push(" }");
            });

            formattedQueries.push(queryBuilder.join("\n"));
            formattedQueries.push(deleteQueryBuilder.join("\n"));
        }
    }

    return formattedQueries;
}

// Find the main target class of a give Shape Graph.
// We determine this by assuming that the main node shape
// is not referenced by any other shape description.
// If more than one is found an exception is thrown.
function extractMainTargetClass(store: RdfStore): Quad_Object {
    const nodeShapes = getSubjects(store, RDF.terms.type, SHACL.terms.NodeShape, null);
    let mainNodeShape = null;

    if (nodeShapes && nodeShapes.length > 0) {
        for (const ns of nodeShapes) {
            const isNotReferenced = getSubjects(store, null, ns, null).length === 0;

            if (isNotReferenced) {
                if (!mainNodeShape) {
                    mainNodeShape = ns;
                } else {
                    throw new Error("There are multiple main node shapes in a given shape."
                        + " Unrelated shapes must be given as separate member shapes");
                }
            }
        }
        if (mainNodeShape) {
            const tcq = getObjects(store, mainNodeShape, SHACL.terms.targetClass, null)[0];
            if (tcq) {
                return tcq;
            } else {
                throw new Error("No target class found in main SHACL Node Shapes");
            }
        } else {
            throw new Error("No main SHACL Node Shapes found in given member shape");
        }
    } else {
        throw new Error("No SHACL Node Shapes found in given member shape");
    }
}