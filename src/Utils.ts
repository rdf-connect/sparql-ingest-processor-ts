import { XSD } from "@treecg/types";
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";

import type { Term, Quad_Subject, Quad_Object } from "@rdfjs/types";

const df = new DataFactory();

export function getSubjects(
    store: RdfStore,
    predicate: Term | null,
    object: Term | null,
    graph?: Term | null,
): Quad_Subject[] {
    return store.getQuads(null, predicate, object, graph).map((quad) => {
        return quad.subject;
    });
}

export function getObjects(
    store: RdfStore,
    subject: Term | null,
    predicate: Term | null,
    graph?: Term | null,
): Quad_Object[] {
    return store.getQuads(subject, predicate, null, graph).map((quad) => {
        return quad.object;
    });
}

export function sanitizeQuads(store: RdfStore): void {
    for (const q of store.getQuads()) {
        // There is an issue with triples like <a> <b> +30.
        // Virtuoso doesn't accept the implicit integer type including the + sign.
        if (q.object.termType === "Literal" && q.object.datatype.value === XSD.integer) {
            if (/\+\d+/.test(q.object.value) && q.object.value.startsWith("+")) {
                store.removeQuad(q);
                store.addQuad(df.quad(
                    q.subject,
                    q.predicate,
                    df.literal(q.object.value.substring(1), df.namedNode(XSD.integer)),
                    q.graph
                ));
            }
        }
    }
}

export async function doSPARQLRequest(query: string, url: string): Promise<void> {
    const res = await fetch(url, {
        method: "POST",
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        body: `update=${fixedEncodeURIComponent(query)}`
    });

    if (!res.ok) {
        throw new Error(`HTTP request failed with code ${res.status} and message: \n${await res.text()}`);
    }
}

function fixedEncodeURIComponent(str: string) {
    return encodeURIComponent(str).replace(/[!'()*]/g, function (c) {
        return '%' + c.charCodeAt(0).toString(16);
    });
}
