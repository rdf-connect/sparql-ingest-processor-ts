# sparql-ingest-processor-ts

[![Build and tests with Node.js](https://github.com/rdf-connect/sparql-ingest-processor-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/sparql-ingest-processor-ts/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/sparql-ingest-processor-ts.svg?style=popout)](https://npmjs.com/package/@rdfc/sparql-ingest-processor-ts)

TypeScript [RDF-Connect](https://rdf-connect.github.io/rdfc.github.io/) processor for ingesting [SDS records](https://treecg.github.io/SmartDataStreams-Spec/) into a SPARQL endpoint.

This processor takes a stream of RDF records, transforms them into [SPARQL Update](https://www.w3.org/TR/sparql11-update/) queries, and executes them against a SPARQL Graph Store via the [SPARQL Protocol](https://www.w3.org/TR/sparql11-protocol/).  
It supports `INSERT DATA`, `DELETE INSERT WHERE`, and `DELETE WHERE` queries, configurable through change semantics or SDS record content.

---

## Usage

### Installation

```bash
npm install
npm run build
```

Or install from NPM:

```bash
npm install @rdfc/sparql-ingest-processor-ts
```

---

### Pipeline Configuration Example

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

### Import the processor definitions
<> owl:imports <./node_modules/@rdfc/sparql-ingest-processor-ts/processors.ttl>.

### Define the channels your processor needs
<in> a rdfc:Reader.
<out> a rdfc:Writer.

### Attach the processor to the pipeline under the NodeRunner
# Add the `rdfc:processor <ingester>` statement under the `rdfc:consistsOf` statement of the `rdfc:NodeRunner`

### Define and configure the processor
<ingester> a rdfc:SPARQLIngest;
    rdfc:memberStream <in>;
    rdfc:ingestConfig [
        rdfc:memberIsGraph false;
        rdfc:memberShape "http://ex.org/Shape1", "http://ex.org/Shape2";
        rdfc:changeSemantics [
            rdfc:changeTypePath "http://ex.org/changeType";
            rdfc:createValue "http://ex.org/Create";
            rdfc:updateValue "http://ex.org/Update";
            rdfc:deleteValue "http://ex.org/Delete"
        ];
        rdfc:targetNamedGraph "http://ex.org/myGraph";
        rdfc:transactionConfig [
            rdfc:transactionIdPath "http://ex.org/transactionId";
            rdfc:transactionEndPath "http://ex.org/transactionEnd"
        ];
        rdfc:graphStoreUrl "http://example.org/sparql";
        rdfc:forVirtuoso false
    ];
    rdfc:sparqlWriter <out>.
```

---

## Configuration

### Parameters of `rdfc:SPARQLIngest`:
- `rdfc:memberStream` (**rdfc:Reader**, required): Input SDS record stream.
- `rdfc:ingestConfig` (**rdfc:IngestConfig**, required): Configuration for ingest behavior.
- `rdfc:sparqlWriter` (**rdfc:Writer**, optional): Output stream of generated SPARQL queries.

---

### Parameters of `rdfc:IngestConfig`:
- `rdfc:memberIsGraph` (**boolean**, required): Whether each SDS record represents a named graph.
- `rdfc:memberShape` (**string**, optional, repeatable): SHACL shape identifiers used to guide query construction when payloads are incomplete.
- `rdfc:changeSemantics` (**rdfc:ChangeSemantics**, optional): Configures mapping between change types (create/update/delete) and SPARQL operations.
- `rdfc:targetNamedGraph` (**string**, optional): Force all operations into a specific named graph (ignored if `memberIsGraph = true`).
- `rdfc:transactionConfig` (**rdfc:TransactionConfig**, optional): Groups records by transaction ID for atomic updates.
- `rdfc:graphStoreUrl` (**string**, optional): SPARQL Graph Store endpoint URL.
- `rdfc:forVirtuoso` (**boolean**, optional): Enables Virtuoso-specific handling.
- `rdfc:accessToken` (**string**, optional): Access token for authenticated graph stores.
- `rdfc:measurePerformance` (**rdfc:PerformanceConfig**, optional): Enables performance measurement of SPARQL queries.

---

### Parameters of `rdfc:ChangeSemantics`:
- `rdfc:changeTypePath` (**string**, required): Predicate identifying the type of change in SDS records.
- `rdfc:createValue` (**string**, required): Value representing a create operation.
- `rdfc:updateValue` (**string**, required): Value representing an update operation.
- `rdfc:deleteValue` (**string**, required): Value representing a delete operation.

---

### Parameters of `rdfc:TransactionConfig`:
- `rdfc:transactionIdPath` (**string**, required): Predicate identifying the transaction ID.
- `rdfc:transactionEndPath` (**string**, required): Predicate marking the last record in a transaction.

---

### Parameters of `rdfc:PerformanceConfig`:
- `rdfc:name` (**string**, required): Name of the performance measurement run.
- `rdfc:outputPath` (**string**, required): File path where performance logs will be written.
- `rdfc:failureIsFatal` (**boolean**, optional): If true, aborts on performance measurement failure.
- `rdfc:queryTimeout` (**integer**, optional): Maximum query execution time in milliseconds.

---

## Example

```turtle
<ingester> a rdfc:SPARQLIngest;
    rdfc:memberStream <in>;
    rdfc:ingestConfig [
        rdfc:memberIsGraph true;
        rdfc:targetNamedGraph "http://example.org/targetGraph";
        rdfc:graphStoreUrl "http://example.org/sparql"
    ];
    rdfc:sparqlWriter <out>.
```

---

## Notes

- Delete operations can be handled differently depending on how complete the SDS record payload is.
- When `memberIsGraph = true`, queries are wrapped with `GRAPH` and `WITH` clauses.
- Transactions can buffer multiple SDS records and commit them together using `rdfc:transactionConfig`.
- SHACL shapes (`rdfc:memberShape`) can be provided to help identify deletion targets when payloads are incomplete.

