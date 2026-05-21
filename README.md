# High-Level Architecture Blueprint

## Project

### Cloud-Native Hybrid Batch + Streaming Music Analytics Platform

---

# Objective

Build a production-style hybrid batch and streaming data platform for music analytics and royalty processing using AWS, Kafka, Spark Structured Streaming, and Delta Lake.

The platform will:

* ingest metadata from YouTube and Spotify APIs,
* process streaming playback/view events,
* store data in a lakehouse architecture,
* generate analytics and royalty reporting datasets.

---

# Architecture Zones

## 1. Source Systems

### Batch Metadata Sources

* YouTube Data API
* Spotify API

Purpose:

* Fetch music metadata
* Artist and track enrichment
* Popularity metrics
* Video and platform metadata

---

## 2. Streaming Event Generation

### Streaming Event Generator

Python-based event producer simulating:

* track plays,
* video views,
* music consumption activity.

Example event:

```json
{
  "event_id": "uuid",
  "track_id": "T100",
  "platform": "YOUTUBE",
  "territory": "US",
  "event_time": "2026-05-21T10:00:00Z"
}
```

Purpose:

* Simulate near real-time music consumption
* Push events continuously into Kafka

---

## 3. Streaming Layer

### Kafka / Confluent Cloud

Topic:

track_usage_events

Purpose:

* Buffer streaming events
* Decouple producers and consumers
* Enable scalable event ingestion

Streaming concepts demonstrated:

* event-driven architecture
* partitions
* offset management
* scalable consumers

---

## 4. Real-Time Processing Layer

### Spark Structured Streaming

Consumes events from Kafka.

Responsibilities:

* parse JSON events
* validate schemas
* deduplicate records
* apply event-time processing
* handle late-arriving events using watermarks
* write events into Bronze Delta layer

Demonstrates:

* micro-batch streaming
* exactly-once semantics
* checkpointing
* fault tolerance

---

## 5. Lakehouse Storage Layer

### AWS S3 + Delta Lake

Storage hierarchy:

bronze/
silver/
gold/

---

### Bronze Layer

Raw ingestion layer.

Contains:

* raw API metadata
* raw streaming events

Purpose:

* immutable raw storage
* replayability
* auditability

---

### Silver Layer

Cleaned and enriched layer.

Responsibilities:

* schema standardization
* metadata enrichment
* deduplication
* business rule validation

---

### Gold Layer

Business-ready analytical datasets.

Contains:

* daily usage aggregates
* platform trends
* territory metrics
* royalty fact tables

Purpose:

* analytics consumption
* reporting
* downstream business intelligence

---

## 6. Batch Processing Layer

### Spark Batch Jobs

Responsibilities:

* incremental processing
* aggregation pipelines
* royalty calculations
* historical reconciliation
* late-event corrections
* SCD Type 2 processing

Key concept:

> Streaming provides freshness.
> Batch provides correctness.

---

## 7. Analytics & Reporting Layer

Outputs:

* daily usage analytics
* royalty reporting tables
* platform comparison metrics
* territory-level reporting

Potential future consumers:

* dashboards
* BI tools
* downstream APIs

---

# High-Level Data Flow

YouTube API ───┐
               │
Spotify API ───┤
               ▼
      Metadata Ingestion
               ▼
          S3 Bronze

Streaming Event Generator
               ▼
            Kafka
               ▼
Spark Structured Streaming
               ▼
      Bronze Usage Events
               ▼
        Spark Batch Jobs
               ▼
      Silver / Gold Layers
               ▼
 Royalty & Analytics Tables

---

# Core Technologies

| Technology                 | Purpose                      |
| -------------------------- | ---------------------------- |
| AWS S3                     | Lakehouse storage            |
| Kafka                      | Streaming ingestion          |
| Spark Structured Streaming | Real-time processing         |
| Delta Lake                 | ACID lakehouse storage       |
| Spark Batch                | Batch transformations        |
| Python                     | Event generation & ingestion |
| Airflow (future)           | Workflow orchestration       |

---

# Concepts Demonstrated

* Hybrid batch + streaming architecture
* Event-driven design
* Lakehouse architecture
* Incremental processing
* SCD Type 2 dimensions
* Event-time processing
* Watermarks and late data handling
* Exactly-once semantics
* Batch reconciliation
* Data quality validation
* Cloud-native storage design
