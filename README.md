# sparql-ingest-processor-ts

[![Build and tests with Node.js](https://github.com/rdf-connect/sparql-ingest-processor-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/sparql-ingest-processor-ts/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/sparql-ingest-processor-ts.svg?style=popout)](https://npmjs.com/package/@rdfc/sparql-ingest-processor-ts)

TypeScript [RDF-Connect](https://rdf-connect.github.io/rdfc.github.io/) processor for ingesting [SDS records](https://treecg.github.io/SmartDataStreams-Spec/) or in general, a stream of RDF quads into a SPARQL endpoint.

This processor takes a stream of RDF records, transforms them into corresponding [SPARQL Update](https://www.w3.org/TR/sparql11-update/) queries, and executes them against a SPARQL Graph Store via the [SPARQL Protocol](https://www.w3.org/TR/sparql11-protocol/).  
It supports `INSERT DATA`, `DELETE INSERT WHERE`, and `DELETE WHERE` queries, configurable through change semantics or SDS record content. It also supports direct quad ingestion via the SPARQL Graph Store Protocol.

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
@prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

### Import the processor definitions
<> owl:imports <./node_modules/@rdfc/sparql-ingest-processor-ts/processors.ttl>.

### Define the channels your processor needs
<in> a rdfc:Reader.
<out> a rdfc:Writer.

### Attach the processor to the pipeline under the NodeRunner
# Add the `rdfc:processor <ingester>` statement under the `rdfc:consistsOf` statement of the `rdfc:NodeRunner`

### Define and configure the processor
<ingester> a rdfcp:SPARQLIngest;
    rdfcp:input <in>;
    rdfcp:ingestConfig [
        rdfcp:operationMode "Replication";
        rdfcp:memberBatchSize 500;
        rdfcp:memberShape "http://ex.org/Shape";
        rdfcp:changeSemantics [
            rdfcp:changeTypePath "http://ex.org/changeType";
            rdfcp:createValue "http://ex.org/Create";
            rdfcp:updateValue "http://ex.org/Update";
            rdfcp:deleteValue "http://ex.org/Delete"
        ];
        rdfcp:targetNamedGraph "http://ex.org/myGraph";
        rdfcp:transactionConfig [
            rdfcp:transactionIdPath "http://ex.org/transactionId";
            rdfcp:transactionEndPath "http://ex.org/transactionEnd"
        ];
        rdfcp:graphStoreUrl "http://example.org/sparql";
        rdfcp:forVirtuoso false;
        rdfcp:accessToken "myAccessToken";
        rdfcp:measurePerformance [
            rdfcp:name "myPerformanceMeasurement";
            rdfcp:outputPath "/path/to/output";
            rdfcp:failureIsFatal true;
            rdfcp:queryTimeout 1800
        ]
    ];
    rdfcp:output <out>.
```

---

## Configuration

### Parameters of `rdfcp:SPARQLIngest`:
- `rdfcp:input` (**rdfc:Reader**, required): Input SDS record stream.
- `rdfcp:ingestConfig` (**rdfcp:IngestConfigShape**, required): Configuration for ingest behavior.
- `rdfcp:output` (**rdfc:Writer**, optional): Output stream of generated SPARQL queries.

---

### Parameters of `rdfcp:IngestConfigShape`:
- `rdfcp:operationMode` (**string**, optional): Operation mode of the processor. Can be `Replication` or `Sync`. `Replication` mode uses the [SPARQL Graph Store Protocol](https://www.w3.org/TR/sparql11-http-rdf-update/) to ingest data directly into a triple store, which assumes that all received data is meant to be added. `Sync` mode uses the [SPARQL UPDATE specification](https://www.w3.org/TR/sparql11-update/) to ingest data via SPARQL queries, which allows for all CRUD operations. Default: `Sync`.
- `rdfcp:memberBatchSize` (**integer**, optional): Number of input records to batch together for ingestion when running in `Replication` mode. Default: `100`.
- `rdfcp:memberShape` (**string**, optional): SHACL shape used to guide query construction when payloads are incomplete.
- `rdfcp:changeSemantics` (**rdfcp:ChangeSemanticsShape**, optional): Configures mapping between change types (create/update/delete) and SPARQL operations.
- `rdfcp:targetNamedGraph` (**string** or IRI, optional): Force all operations into a specific named graph.
- `rdfcp:transactionConfig` (**rdfcp:TransactionConfigShape**, optional): Groups records by transaction ID for atomic updates.
- `rdfcp:graphStoreUrl` (**string** or IRI, optional): SPARQL Graph Store endpoint URL.
- `rdfcp:forVirtuoso` (**boolean**, optional): Enables Virtuoso-specific handling to avoid query size limits. Default: `false`.
- `rdfcp:accessToken` (**string**, optional): Access token for authenticated graph stores.
- `rdfcp:measurePerformance` (**rdfcp:PerformanceConfigShape**, optional): Enables performance measurement of SPARQL queries.

---

### Parameters of `rdfcp:ChangeSemanticsShape`:
- `rdfcp:changeTypePath` (**string** or IRI, required): Predicate identifying the type of change in SDS records.
- `rdfcp:createValue` (**string** or IRI, required): Value representing a create operation.
- `rdfcp:updateValue` (**string** or IRI, required): Value representing an update operation.
- `rdfcp:deleteValue` (**string** or IRI, required): Value representing a delete operation.

---

### Parameters of `rdfcp:TransactionConfigShape`:
- `rdfcp:transactionIdPath` (**string** or IRI, required): Predicate identifying the transaction ID.
- `rdfcp:transactionEndPath` (**string** or IRI, required): Predicate marking the last record in a transaction.

---

### Parameters of `rdfcp:PerformanceConfigShape`:
- `rdfcp:name` (**string**, required): Name of the performance measurement run.
- `rdfcp:outputPath` (**string** or IRI, required): Existing folder where the performance log is written, as `<rdfcp:name>.json`.
- `rdfcp:failureIsFatal` (**boolean**, optional): If true, aborts on performance measurement failure. Default: `false`.
- `rdfcp:queryTimeout` (**integer**, optional): Maximum query execution time in seconds. Default: `1800` (30 minutes), also when `rdfcp:measurePerformance` is not set.

---

## Example

```turtle
<ingester> a rdfcp:SPARQLIngest;
    rdfcp:input <in>;
    rdfcp:ingestConfig [
        rdfcp:targetNamedGraph "http://example.org/targetGraph";
        rdfcp:graphStoreUrl "http://example.org/sparql"
    ];
    rdfcp:output <out>.
```

---

## Notes

- Delete operations can be handled differently depending on how complete the input record is. A SHACL shape (`rdfcp:memberShape`) can be provided to help identify deletion targets when payloads are incomplete.
- Transactions can buffer multiple input records and commit them together using `rdfcp:transactionConfig`.

