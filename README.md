# sparql-ingest-processor-ts

[![Bun CI](https://github.com/julianrojas87/xml-utils-processors-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/julianrojas87/xml-utils-processors-ts/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/xml-utils-processors-ts.svg?style=popout)](https://npmjs.com/package/xml-utils-processors-ts)

Typescript [Connector Architecture](https://the-connector-architecture.github.io/site/docs/1_Home) processor for producing the corresponding SPARQL Update queries that write a stream of [SDS records](https://treecg.github.io/SmartDataStreams-Spec/) into a SPARQL triple store. Currently this repository exposes one function:

### [`js:SPARQLIngest`](https://github.com/julianrojas87/sparql-ingest-processor-ts/blob/main/processors.ttl#L9)

This processor is able to take an input stream of SDS records (described by the `sds:stream` and `sds:payload` properties) and produce corresponding [SPARQL Update](https://www.w3.org/TR/sparql11-update/) queries (`INSERT DATA`, `DELETE INSERT WHERE` and `DELETE WHERE`) to be executed over a graph store via the [SPARQL protocol](https://www.w3.org/TR/sparql11-protocol/).

By default, this processor will produce a `DELETE INSERT WHERE` query that will overwrite all the triples present in the payload of the received SDS record. However, specific query operations can be generated based on configurable change semantics that can be included in the SDS record payload. Next, an example of this processor is shown with a configuration that specifies the predicate `ex:changeType` and the values `as:Create`, `as:Update` and `as:Delete` as the expected values for generating `INSERT DATA`, `DELETE INSERT WHERE` ans `DELETE WHERE` queries respectively.

```turtle
[ ] a js:SPARQLIngest; 
    js:memberStream <inputStream>;
    js:ingestConfig [
        js:memberIsGraph false;
        js:memberShape "Some SHACL shape", "Another SHACL shape";
        js:changeSemantics [
            js:changeTypePath "http://ex.org/changeType";
            js:createValue "http://ex.org/Create";
            js:updateValue "http://ex.org/Update";
            js:deleteValue "http://ex.org/Delete"
        ];
        js:targetNamedGraph "http://ex.org/myGraph";
        js:transactionIdPath "http://ex.org/trancationId"
    ];
    js:sparqlWriter <outputStream>.
```

For the case of delete operations, additional information can be provided depending on the content of the SDS record payload signaling a delete:

1. The payload is complete and contains all the triples that must be deleted from the triple store. In this case no additional information is needed.
2. The payload only contains the type of the payload's main entity (or member) via `rdf:type`. In this case one or more SHACL shapes can be configured via the `js:memberShape` property. The processor will identify the corresponding shape of an input SDS record (via the shape's target class) and the proper query pattern will be generated.
3. The payload does not contain the type of the payload's main entity (or member). In this case, is not possible to identify the corresponding SHACL shape, therefore a query reflecting all shapes via `OPTIONAL` clauses will be generated.

In case that the main entity (member) of the SDS record payload is always a named graph, this can be configured by setting the `js:memberIsGraph` to `true`. In this scenario, all resulting queries will be properly set with the `GRAPH` and the `WITH` clauses.

If a specific named graph should be targeted by all the resulting SPARQL Update queries, this can be configured via the `js:targetNamedGraph` property. This property will be ignored if the `js:memberIsGraph` property is `true`.

Lastly, the main entity (member) of SDS record payload may contain a transaction ID when the member is part of a larger group of members that must be updated altogether into the targeted triple store. This particular property can be indicated to the processor via the `js:transactionIdPath` configuration property. The processor will proceed to buffer all records containing the same transaction ID and execute the corresponding SPARQL Update query for all members at once.
