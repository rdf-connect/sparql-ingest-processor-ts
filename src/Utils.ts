import { XSD } from "@treecg/types";
import { Store, DataFactory as DF } from "n3";

export function sanitizeQuads(store: Store): void {
    for (const q of store.getQuads(null, null, null, null)) {
        if (q.object.termType === "Literal") {
            if (/\+\d+/.test(q.object.value)) {
                store.removeQuad(q);
                store.addQuad(q.subject, q.predicate, DF.literal(q.object.value.substring(1), DF.namedNode(XSD.integer)), q.graph);
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
        body: `query=${encodeURIComponent(query)}`
    });

    if (!res.ok) {
        throw new Error(`HTTP request failed with code ${res.status} and message: \n${await res.text()}`);
    }
}