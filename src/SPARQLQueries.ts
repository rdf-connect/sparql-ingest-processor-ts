import { Quad_Object } from "@rdfjs/types";
import { RDF, SHACL } from "@treecg/types";
import { Writer as N3Writer, Parser } from "n3"
import { RdfStore } from "rdf-stores";
import { DataFactory } from "rdf-data-factory";
import { getObjects, getSubjects, splitStore } from "./Utils";

const df = new DataFactory();

export const CREATE = (store: RdfStore, namedGraph?: string, multipleNamedGraphs?: boolean): string => {
    // TODO: Handle case of multiple members being Named Graphs
    
    // Split the query into multiple queries to avoid query length limits 
    // (such as the 10000 SQL code lines in Virtuoso) for large inserts.
    // 500 is an empirically obtained value to avoid exceeding the 10000 lines limit in Virtuoso
    const stores = splitStore(store, 500);

    return `
        ${stores.map((subStore, i) => {
            return `
                ${namedGraph ? `WITH <${namedGraph}>` : ""}
                INSERT DATA { 
                    ${new N3Writer().quadsToString(subStore.getQuads())} 
                }
                ${i === stores.length - 1 ? "" : ";"}
            `;
        }).join("\n")}
    `;
};

// We have to use a multiple query request of a DELETE WHERE + INSERT DATA for the default update operation
// because some triple stores like Virtuoso fail on executing a DELETE INSERT WHERE when there is no data to delete.
export const UPDATE = (store: RdfStore, namedGraph?: string, multipleNamedGraphs?: boolean): string => {
    // TODO: Handle case of multiple members being Named Graphs
    const formattedQuery = formatQuery(store);

    // Split the query into multiple queries to avoid query length limits 
    // (such as the 10000 SQL code lines in Virtuoso) for large inserts.
    // 500 is an empirically obtained value to avoid exceeding the 10000 lines limit in Virtuoso
    const stores = splitStore(store, 500);
    return `
        ${namedGraph ? `WITH <${namedGraph}>` : ""}
        DELETE { 
            ${formattedQuery[0]} 
        }
        WHERE { 
            ${formattedQuery[0]} 
        };
        ${stores.map((subStore, i) => {
            return `
                ${namedGraph ? `WITH <${namedGraph}>` : ""}
                INSERT DATA { 
                    ${new N3Writer().quadsToString(subStore.getQuads())} 
                }
                ${i === stores.length - 1 ? "" : ";"}
            `;
        }).join("\n")}
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
        const formatted = formatQuery(store, memberIRI, memberShapes, indexStart);
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

function formatQuery(
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
        for (const quad of memberStore.getQuads()) {
            if (!subjectSet.has(quad.subject.value)) {
                // Make sure every subject is processed only once
                subjectSet.add(quad.subject.value);
                if (quad.subject.termType === "NamedNode") {
                    // Define a pattern that covers every property and value of named nodes
                    queryBuilder.push(`<${quad.subject.value}> ?p_${i} ?o_${i}.`);
                } else if (quad.subject.termType === "BlankNode") {
                    // Define a pattern that covers the referencing BGP and every property and value of blank nodes
                    queryBuilder.push(`?bn_${i} <${quad.predicate.value}> ${
                        quad.object.termType === "Literal" ? `"${quad.object.value}"^^<${quad.object.datatype.value}>` 
                        : quad.object.termType === "BlankNode" ? `?bn_ref_${i}` 
                        : `<${quad.object.value}>`
                    }.`);
                    queryBuilder.push(`?bn_${i} ?p_${i} ?o_${i}.`);
                    queryBuilder.push(`?s_ref_${i} ?p_ref_${i} ?bn_${i}.`);
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
        const memberType = getObjects(memberStore, df.namedNode(memberIRI!), RDF.terms.type)[0];
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