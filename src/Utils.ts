
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