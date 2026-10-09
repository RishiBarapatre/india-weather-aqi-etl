"""
scripts/generate_interview_guide.py
-----------------------------------
Generates a comprehensive, professional Data Engineering Interview Preparation Guide
for the India Weather & AQI ETL Pipeline project using ReportLab.
"""

import sys
from pathlib import Path
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether, HRFlowable
)
from reportlab.pdfgen import canvas


class NumberedCanvas(canvas.Canvas):
    """
    Two-pass canvas to dynamically compute and display 'Page X of Y' in the running footer.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_header_footer(num_pages)
            super().showPage()
        super().save()

    def draw_header_footer(self, page_count):
        self.saveState()
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#718096"))

        # Running header on pages > 1
        if self._pageNumber > 1:
            self.drawString(
                54, letter[1] - 36,
                "India Weather & AQI ETL Pipeline | Data Engineering Interview Guide"
            )
            self.setStrokeColor(colors.HexColor("#E2E8F0"))
            self.setLineWidth(0.5)
            self.line(54, letter[1] - 42, letter[0] - 54, letter[1] - 42)

        # Running footer on all pages
        page_str = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(letter[0] - 54, 30, page_str)
        self.drawString(54, 30, "CONFIDENTIAL - DATA ENGINEERING INTERVIEW PREPARATION GUIDE")
        self.setStrokeColor(colors.HexColor("#E2E8F0"))
        self.setLineWidth(0.5)
        self.line(54, 40, letter[0] - 54, 40)

        self.restoreState()


def build_pdf(filename: str):
    doc = SimpleDocTemplate(
        filename,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=54,
        bottomMargin=54,
    )

    styles = getSampleStyleSheet()

    # Custom styles
    primary_color = colors.HexColor("#1A365D")   # Deep Navy
    secondary_color = colors.HexColor("#2B6CB0") # Slate Blue
    dark_neutral = colors.HexColor("#2D3748")    # Charcoal
    muted_neutral = colors.HexColor("#4A5568")   # Grey
    code_bg = colors.HexColor("#F7FAFC")
    box_border = colors.HexColor("#CBD5E0")
    callout_bg = colors.HexColor("#EDF2F7")

    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=24,
        leading=28,
        textColor=primary_color,
        spaceAfter=6,
    )

    subtitle_style = ParagraphStyle(
        'DocSubTitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=16,
        textColor=secondary_color,
        spaceAfter=14,
    )

    h1_style = ParagraphStyle(
        'H1',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=15,
        leading=18,
        textColor=primary_color,
        spaceBefore=14,
        spaceAfter=8,
        keepWithNext=True,
    )

    h2_style = ParagraphStyle(
        'H2',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=14,
        textColor=secondary_color,
        spaceBefore=10,
        spaceAfter=4,
        keepWithNext=True,
    )

    h3_style = ParagraphStyle(
        'H3',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=9.5,
        leading=12,
        textColor=dark_neutral,
        spaceBefore=6,
        spaceAfter=3,
        keepWithNext=True,
    )

    body_style = ParagraphStyle(
        'Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8.5,
        leading=12,
        textColor=dark_neutral,
        spaceAfter=6,
    )

    bullet_style = ParagraphStyle(
        'Bullet',
        parent=body_style,
        leftIndent=12,
        firstLineIndent=-8,
        spaceAfter=4,
    )

    code_style = ParagraphStyle(
        'CodeBlock',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=7.5,
        leading=10,
        textColor=colors.HexColor("#1A202C"),
    )

    q_style = ParagraphStyle(
        'Question',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#742A2A"), # Deep Wine / Red
    )

    ans_style = ParagraphStyle(
        'Answer',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8.2,
        leading=11.5,
        textColor=dark_neutral,
    )

    def callout_box(text: str, title: str = "Key Defense Point"):
        p_title = Paragraph(f"<b>{title}</b>", h3_style)
        p_body = Paragraph(text, body_style)
        t = Table([[p_title], [p_body]], colWidths=[letter[0] - 108])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), callout_bg),
            ('BOX', (0, 0), (-1, -1), 0.75, box_border),
            ('PADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 2),
        ]))
        return t

    def code_box(code_text: str):
        lines = [Paragraph(line.replace(' ', '&nbsp;').replace('<', '&lt;').replace('>', '&gt;'), code_style) for line in code_text.strip().split('\n')]
        t = Table([[lines]], colWidths=[letter[0] - 108])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), code_bg),
            ('BOX', (0, 0), (-1, -1), 0.5, box_border),
            ('PADDING', (0, 0), (-1, -1), 6),
        ]))
        return t

    def qa_block(q_num: int, question: str, defense_core: str, answer_points: list[str]):
        content = []
        content.append(Paragraph(f"<b>Q{q_num}: {question}</b>", q_style))
        content.append(Spacer(1, 3))
        content.append(Paragraph(f"<b>Core Defense:</b> {defense_core}", ParagraphStyle(
            'CoreDef', parent=ans_style, fontName='Helvetica-Oblique', textColor=secondary_color
        )))
        content.append(Spacer(1, 3))
        for pt in answer_points:
            content.append(Paragraph(f"&bull; {pt}", ParagraphStyle(
                'AnsBullet', parent=ans_style, leftIndent=8, firstLineIndent=-5, spaceAfter=2
            )))
        
        t = Table([[content]], colWidths=[letter[0] - 108])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor("#FFFFFF")),
            ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor("#E2E8F0")),
            ('PADDING', (0, 0), (-1, -1), 6),
        ]))
        return t

    story = []

    # =========================================================================
    # COVER / HEADER
    # =========================================================================
    story.append(Spacer(1, 10))
    story.append(Paragraph("Data Engineering Interview Preparation Guide", title_style))
    story.append(Paragraph("India Weather & Air Quality Index (AQI) ETL Pipeline | Project Defense & Knowledge Mastery", subtitle_style))
    story.append(HRFlowable(width="100%", thickness=1.5, color=primary_color, spaceBefore=0, spaceAfter=12))

    meta_table = Table([
        [
            Paragraph("<b>Target Role:</b> Data Engineer / Associate DE (2026)", body_style),
            Paragraph("<b>Focus:</b> Airflow, Postgres DWH, MongoDB Lake, Data Quality", body_style),
        ],
        [
            Paragraph("<b>Author:</b> Rishi Barapatre", body_style),
            Paragraph("<b>Repo:</b> github.com/RishiBarapatre/india-weather-aqi-etl", body_style),
        ]
    ], colWidths=[(letter[0]-108)/2, (letter[0]-108)/2])
    meta_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor("#F7FAFC")),
        ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor("#CBD5E0")),
        ('PADDING', (0, 0), (-1, -1), 5),
    ]))
    story.append(meta_table)
    story.append(Spacer(1, 14))

    # =========================================================================
    # SECTION 1: HOW TO PITCH THIS PROJECT
    # =========================================================================
    story.append(Paragraph("1. How to Pitch This Project (The Elevator Speeches)", h1_style))
    story.append(Paragraph(
        "Interviewers typically open with: <i>'Walk me through your most recent project'</i> or <i>'Tell me about an ETL pipeline you built.'</i> "
        "Do not list tools chronologically; use the <b>STAR method</b> (Situation, Task, Action, Result) focusing on engineering trade-offs.",
        body_style
    ))

    story.append(Paragraph("The 60-Second High-Impact Pitch", h2_style))
    p_pitch60 = (
        "<i>\"I built an automated, production-grade ETL pipeline that ingests live meteorological data from Open-Meteo and air quality observations "
        "from the World Air Quality Index project for 35 Indian cities on a 3-hour schedule, modeling it into a Star Schema data warehouse in PostgreSQL. "
        "The core technical challenge was handling real-world data imperfections: WAQI has no batch endpoint and experiences per-station schema drift and downtime, "
        "while legacy municipal records contain historical city names and invalid coordinates. "
        "I designed a Medallion architecture using MongoDB as a raw Bronze landing zone to preserve verbatim API payloads, orchestrated dynamic task mapping in Airflow 2.9 "
        "to isolate per-city failures, built a 3-tier entity resolution engine to reconcile city aliases, and established an automated 10-check data quality gate "
        "that logs audit metrics and halts ingestion on critical anomalies. The entire warehouse load is strictly idempotent with zero duplicate records across reruns.\"</i>"
    )
    story.append(callout_box(p_pitch60, "Practice This Pitch Word-for-Word"))
    story.append(Spacer(1, 10))

    story.append(Paragraph("The 2-Minute Deep-Dive STAR Breakdown", h2_style))
    star_data = [
        [Paragraph("<b>S - Situation</b>", body_style), Paragraph("Urban Indian centers face severe seasonal air pollution and weather extremes, but environmental data is fragmented across APIs with different rate limits, missing stations, and inconsistent city naming standards.", body_style)],
        [Paragraph("<b>T - Task</b>", body_style), Paragraph("Build an end-to-end data platform that extracts, cleans, validates, and stores this data into an analytical warehouse while ensuring fault tolerance, idempotency, and automated data quality auditing.", body_style)],
        [Paragraph("<b>A - Action</b>", body_style), Paragraph("1. Landed raw API JSON in MongoDB (Bronze zone) to decouple extraction from transformations.<br/>2. Orchestrated dynamic task mapping in Apache Airflow for independent per-city execution and retries.<br/>3. Implemented a 3-tier city resolver (exact, alias lookup, Levenshtein distance) to handle historical naming (e.g., Bombay -> Mumbai).<br/>4. Modeled a Kimball Star Schema with pre-generated dim_date and PostgreSQL idempotent ON CONFLICT upserts.<br/>5. Implemented a 10-point automated DQ check suite recording results to an audit table in a single transaction.", body_style)],
        [Paragraph("<b>R - Result</b>", body_style), Paragraph("Successfully loaded live weather and AQI facts for 34 Indian cities, processed dirty data with 100% test coverage across 77 unit tests, prevented warehouse poisoning via quarantine tables, and delivered analytical queries for rolling 24-hour averages and pollution streaks.", body_style)],
    ]
    star_table = Table(star_data, colWidths=[100, letter[0] - 108 - 100])
    star_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor("#FFFFFF")),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#CBD5E0")),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('PADDING', (0, 0), (-1, -1), 5),
    ]))
    story.append(star_table)
    story.append(Spacer(1, 14))

    # =========================================================================
    # SECTION 2: CORE TECHNICAL TOPICS TO MASTER
    # =========================================================================
    story.append(Paragraph("2. Core Technical Domains You Must Understand", h1_style))
    story.append(Paragraph("Before walking into an interview, ensure you can explain the core mechanics of each domain:", body_style))

    story.append(Paragraph("Domain A: Data Architecture & Storage Layers (Medallion Pattern)", h2_style))
    story.append(Paragraph(
        "<b>Bronze (Raw Landing Zone):</b> MongoDB. Why? Raw JSON responses change over time. If a sensor stops reporting SO2 or WAQI uses a misspelled field (e.g. 'dominentpol'), "
        "a relational schema with strict DDL would fail during extraction. Storing documents verbatim guarantees zero data loss at ingestion and allows re-parsing historical data if transformation rules change.<br/>"
        "<b>Silver (Cleaning & Conformance):</b> In-memory processing via Pandas inside Airflow workers. Standardizes timestamps to ISO-8601 UTC, sanitizes sentinel null tokens ('-', 'N/A', 999999), and resolves city foreign keys.<br/>"
        "<b>Gold (Curated Data Warehouse):</b> PostgreSQL Star Schema. Facts and dimensions with primary keys, foreign keys, and analytical indexes for reporting tools and BI.",
        body_style
    ))

    story.append(Paragraph("Domain B: Dimensional Modeling (Kimball Methodology)", h2_style))
    story.append(Paragraph(
        "<b>Star Schema vs. 3NF:</b> OLTP systems use 3NF (3rd Normal Form) to eliminate write redundancy. Data Warehouses use Star Schema (denormalized dimensions connected to central fact tables) "
        "to optimize read performance, minimize joins, and make schemas intuitive for analysts.<br/>"
        "<b>Surrogate Keys vs. Natural Keys:</b> We use auto-incrementing integer surrogate keys (`city_id`, `date_key`, `reading_id`) instead of natural keys (like city name strings or timestamps). "
        "Surrogate keys decouple the warehouse from source system changes, protect against natural key mutations, and offer superior join performance in B-tree indexes.<br/>"
        "<b>Role of the Date Dimension (`dim_date`):</b> Pre-populating 4,000+ dates (2020-2030) with pre-calculated calendar attributes (`year`, `quarter`, `week_of_year`, `is_weekend`) "
        "eliminates runtime CPU-heavy `EXTRACT()` and `DATE_TRUNC()` operations and enables outer joins to detect days where stations failed to record observations.<br/>"
        "<b>SCD Concepts:</b> `dim_city` currently implements SCD Type 1 (overwrite). In an interview, explain that SCD Type 2 would introduce `effective_date`, `end_date`, and `is_current` columns to track historical changes (e.g., city population or tier promotions over time).",
        body_style
    ))

    story.append(Paragraph("Domain C: Airflow Orchestration & Pipeline Resilience", h2_style))
    story.append(Paragraph(
        "<b>Dynamic Task Mapping:</b> Feature introduced in Airflow 2.3 (`.expand()`). Allows the number of tasks to be determined at runtime based on the upstream output of `get_active_cities`. "
        "Essential for per-station API calls because each city becomes its own task instance with its own retry state and timeout.<br/>"
        "<b>Idempotency:</b> An ETL pipeline is idempotent if executing it multiple times with the same input produces the exact same state as executing it once. "
        "We achieve this in MongoDB via upserts on compound key `(source, city_id, run_id)` and in PostgreSQL via `ON CONFLICT (city_id, reading_ts) DO UPDATE`.<br/>"
        "<b>Failure Isolation:</b> Transient network errors trigger Airflow task retries with exponential backoff (`retries=3, retry_delay=2m`). Non-transient errors (dead stations) are caught in Python "
        "and logged as structured failure dictionaries rather than bubbling unhandled exceptions, allowing the remaining 33 cities to complete successfully.",
        body_style
    ))

    story.append(Paragraph("Domain D: Advanced SQL Techniques", h2_style))
    story.append(Paragraph(
        "<b>Window Functions vs. GROUP BY:</b> `GROUP BY` collapses rows; window functions (`OVER (...)`) compute aggregate values while preserving individual row identity.<br/>"
        "<b>`RANGE` vs. `ROWS` Frame:</b> `ROWS BETWEEN 24 PRECEDING AND CURRENT ROW` counts physical rows (if readings are missing, you average across days). "
        "`RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW` filters strictly by the timestamp value in the ordering column, handling variable observation intervals correctly.<br/>"
        "<b>Gaps-and-Islands (`rn - rn2`):</b> A classic SQL interview challenge. Row number across all rows (`rn`) minus row number across qualifying rows (`rn2`) produces an invariant constant "
        "that remains identical throughout an unbroken consecutive streak, then jumps when a gap occurs.",
        body_style
    ))

    story.append(PageBreak())

    # =========================================================================
    # SECTION 3: DEFENDING ARCHITECTURAL CHOICES
    # =========================================================================
    story.append(Paragraph("3. Defending Architectural Choices (The 'Why' Questions)", h1_style))
    story.append(Paragraph("Interviewers will probe your architectural justification. Never say <i>'because the tutorial said so'</i>. Use these explicit trade-off arguments:", body_style))

    arch_defenses = [
        (
            1,
            "Why did you use MongoDB for raw ingestion instead of staging tables in PostgreSQL?",
            "Decoupling ingestion from schema enforcement protects against external API drift.",
            [
                "Third-party APIs are external dependencies outside our control. If WAQI adds new pollutant fields or changes nested structure, a relational table with fixed columns would require DDL schema migrations or fail ingestion.",
                "MongoDB stores the raw payload verbatim (including API response headers and execution metadata). If a downstream business rule is modified 6 months later, the original source data is preserved in Bronze to re-run historical transforms without calling the API again.",
                "Separates ingestion latency from transactional warehouse locking."
            ]
        ),
        (
            2,
            "Why did you run two separate PostgreSQL containers instead of one database with multiple schemas?",
            "Resource isolation and operational independence between workflow metadata and analytical workload.",
            [
                "Airflow's internal database stores task heartbeats, execution states, and connection credentials. It experiences high-frequency concurrent writes from the scheduler and workers.",
                "The Analytical Data Warehouse (`warehouse-postgres`) stores dimensional tables and fact data subjected to heavy analytical queries and window aggregates.",
                "Coupling both onto the same database instance is a known anti-pattern: an unoptimized analytical query in your DWH could lock system resources or max out CPU, causing the Airflow scheduler to drop heartbeats and mark running tasks as dead."
            ]
        ),
        (
            3,
            "Why use Airflow Dynamic Task Mapping instead of a Python `for` loop inside a single task?",
            "Granular task observability, concurrency control, and blast-radius containment.",
            [
                "In a single task with a loop, if city #30 of 35 fails or times out, the entire task fails. You would have to re-fetch cities 1 to 29 on retry, wasting network bandwidth and risking API rate limits.",
                "Dynamic Task Mapping (`expand(city=cities)`) creates 34 discrete Airflow TaskInstances. Each city is retried independently.",
                "Airflow UI gives instantaneous visual feedback on exactly which city station failed, and tasks can execute concurrently across worker threads."
            ]
        ),
        (
            4,
            "Why batch Open-Meteo in groups of 10 instead of all 35 at once or 1 by 1?",
            "Balanced network payload vs. failure blast radius and URL length safety.",
            [
                "Open-Meteo allows comma-separated latitude and longitude pairs. Ingesting one-by-one causes 35 separate HTTP handshakes, creating socket overhead and latency.",
                "Passing all 35+ coordinates in a single GET query approaches HTTP URL length thresholds (2,048 characters) and creates an 'all-or-nothing' failure risk.",
                "Batching in chunks of 10 gives the optimal balance: only 4 network calls total, short URL strings, and if one batch fails, 75% of cities still ingest successfully."
            ]
        ),
        (
            5,
            "Why build custom Python Data Quality checks instead of using Great Expectations or dbt test?",
            "Zero framework overhead, direct transaction binding, and complete test transparency.",
            [
                "Great Expectations is a heavy dependency requiring extensive JSON context configs and multi-megabyte artifact storage. In an entry/junior role, interviewers want to see that you can write SQL and Python validation logic yourself.",
                "Our custom runner executes all 10 checks within the same transaction that writes to `pipeline_run_log` and `dq_check_results`.",
                "Plain Python functions are easily unit-tested with standard `unittest.mock` (as demonstrated by our 24 passing unit tests in `test_dq_checks.py`) without running external engines."
            ]
        ),
        (
            6,
            "How does your pipeline ensure true idempotency?",
            "Deterministic natural key constraints coupled with SQL upsert semantics.",
            [
                "Both `fact_weather_reading` and `fact_air_quality_reading` feature a compound UNIQUE constraint on `(city_id, reading_ts)`.",
                "The loader executes `INSERT INTO fact_... VALUES (...) ON CONFLICT (city_id, reading_ts) DO UPDATE SET temperature_c = EXCLUDED.temperature_c, ...`.",
                "If Airflow re-triggers a task due to a worker restart or backfill, existing records are updated in place rather than generating duplicate rows."
            ]
        ),
    ]

    for q_item in arch_defenses:
        story.append(qa_block(q_item[0], q_item[1], q_item[2], q_item[3]))
        story.append(Spacer(1, 5))


    # =========================================================================
    # SECTION 4: TOP 20 INTERVIEW QUESTIONS & MODEL ANSWERS
    # =========================================================================
    story.append(Paragraph("4. High-Probability Interview Questions & Model Answers", h1_style))
    story.append(Paragraph("Study these questions categorized by the core competencies recruiters evaluate:", body_style))

    cat_questions = [
        (
            "Category 1: API Ingestion & Network Resilience",
            [
                (
                    "What happens if the WAQI API returns a 500 error or times out for Delhi?",
                    "Layered resilience: Tenacity retry inside Airflow task -> structured failure catch.",
                    [
                        "First, the `@retry_on_http_error` decorator (powered by Tenacity) performs 3 exponential backoff retries with jitter directly at the HTTP request layer.",
                        "If all 3 attempts fail, the exception is caught inside `_fetch_aqi()`. The task logs the error and returns `{'city_id': 1, 'status': 'error', 'stored': False}`.",
                        "Airflow records that individual mapped task as handled, increments the `cities_failed` counter in `pipeline_run_log`, and continues processing the remaining 33 cities without terminating the entire pipeline run."
                    ]
                ),
                (
                    "How do you handle API rate limits if you scale from 35 cities to 1,000 cities?",
                    "Architectural evolution: Connection pooling, rate limit throttling, and worker pools.",
                    [
                        "In Airflow, configure an Airflow Pool (`waqi_api_pool`) with limited concurrency slots (e.g., 5 parallel tasks) to prevent flooding the endpoint.",
                        "Implement token-bucket rate limiting or Redis-backed distributed semaphores.",
                        "For large enterprise scale, decouple extraction into an asynchronous message queue (e.g., AWS SQS or Kafka) where worker nodes consume at a controlled rate matching the API provider's tier."
                    ]
                )
            ]
        ),
        (
            "Category 2: Data Cleaning & Entity Resolution",
            [
                (
                    "Walk me through how your pipeline handles a messy city name like 'Bombay' or 'bengaluru'.",
                    "Three-tier deterministic-to-probabilistic resolution pipeline.",
                    [
                        "Step 1 (Normalization): Strip whitespace and normalize case to Title Case ('Bombay', 'Bengaluru').",
                        "Step 2 (Exact Alias Match): Query `dim_city_alias`. 'Bombay' matches the raw alias for canonical city 'Mumbai' (`canonical_city_id = 14`). It resolves in O(1) time via B-tree index.",
                        "Step 3 (Fuzzy Match): If unknown, RapidFuzz computes the token-sort ratio against `dim_city`. If score >= 90, the match is accepted and automatically inserted into `dim_city_alias` with `match_method='fuzzy'` so future runs resolve instantly.",
                        "Step 4 (Quarantine): If score < 90, the raw name is written to `quarantine_unmatched_city_names` and the record is dropped for this cycle to prevent corrupting analytics."
                    ]
                ),
                (
                    "Why not just use fuzzy matching on every record every time?",
                    "Performance optimization and defense against false-positive string collisions.",
                    [
                        "Fuzzy matching (Levenshtein distance) is CPU-intensive (O(N*M) string comparison). Running it on thousands of rows per run would degrade pipeline throughput.",
                        "The alias table acts as a persistent lookup cache. Once resolved, subsequent runs execute via indexed hash/B-tree lookups.",
                        "Prevents false positives on geographically close names (e.g., 'Kalyan' vs 'Kalyani')."
                    ]
                )
            ]
        ),
        (
            "Category 3: Data Quality & Governance",
            [
                (
                    "What is the difference between a WARN check and a FAIL check in your DQ runner?",
                    "Severity classification based on physical impossibility vs. transient statistical anomaly.",
                    [
                        "FAIL Checks (Blockers): Indicate corrupt or impossible data. Examples: Humidity < 0% or > 100%, NULL primary keys, missing schema columns, duplicate natural keys. A failure raises `RuntimeError`, turning the Airflow task red and stopping downstream processes.",
                        "WARN Checks (Alerts): Indicate potential environmental extremes or partial missingness that should be audited but shouldn't halt the pipeline. Examples: Temperature > 50°C (could be a legitimate heatwave in Rajasthan), AQI > 400 (severe smog event in Delhi), or 15% of stations offline."
                    ]
                ),
                (
                    "Where do you store data quality metrics, and how would you query them?",
                    "Audited in `dq_check_results` and `pipeline_run_log`.",
                    [
                        "Every execution writes to `dq_check_results` with `check_name`, `table_name`, `status` ('pass'/'warn'/'fail'), and descriptive text.",
                        "To monitor system health over the last 30 days: `SELECT check_name, status, COUNT(*) FROM dq_check_results WHERE checked_at >= NOW() - INTERVAL '30 days' GROUP BY check_name, status;`."
                    ]
                )
            ]
        ),
        (
            "Category 4: Scaling & System Evolution",
            [
                (
                    "How would you redesign this architecture if readings arrived every 10 seconds instead of every 3 hours?",
                    "Transition from Batch Orchestration to Event-Driven Streaming.",
                    [
                        "Replace Airflow scheduled tasks with an event-driven ingestion service (e.g., Kafka producers polling or receiving webhooks).",
                        "Kafka topics: `weather.readings.raw` and `aqi.readings.raw`.",
                        "Stream Processing: Apache Flink or Spark Structured Streaming consuming from Kafka, computing 10-minute tumbling and sliding window aggregates, performing stateful deduplication via RocksDB state stores.",
                        "Storage: Stream raw events to Iceberg/Delta Lake tables on S3/GCS and sink real-time aggregates to a time-series database (TimescaleDB or ClickHouse)."
                    ]
                ),
                (
                    "How would you deploy this to production in AWS or GCP?",
                    "Cloud-native managed service mapping.",
                    [
                        "Orchestration: AWS MWAA (Managed Workflows for Apache Airflow) or GCP Cloud Composer.",
                        "Raw Lake: Amazon S3 or Google Cloud Storage (replacing MongoDB with parquet objects in S3).",
                        "Data Warehouse: Amazon Redshift, Snowflake, or Google BigQuery.",
                        "Secrets: AWS Secrets Manager / GCP Secret Manager for the WAQI token.",
                        "CI/CD: GitHub Actions running pytest, Docker container image builds pushed to ECR/GCR."
                    ]
                )
            ]
        )
    ]

    q_count = 7
    for cat_title, q_list in cat_questions:
        story.append(Paragraph(cat_title, h2_style))
        for q_tuple in q_list:
            story.append(qa_block(q_count, q_tuple[0], q_tuple[1], q_tuple[2]))
            story.append(Spacer(1, 5))
            q_count += 1

    story.append(PageBreak())

    # =========================================================================
    # SECTION 5: ADVANCED SQL PRACTICE & WALKTHROUGH
    # =========================================================================
    story.append(Paragraph("5. Advanced SQL Mastery & Code Defense", h1_style))
    story.append(Paragraph(
        "Data Engineering interviews almost always include a live SQL screening. Interviewers will test your understanding of window frames and streak detection. "
        "Here is the line-by-line mathematical explanation of your analytical queries:",
        body_style
    ))

    story.append(Paragraph("Problem 1: The Gaps-and-Islands Math (`rn - rn2`)", h2_style))
    story.append(Paragraph(
        "<b>Interview Question:</b> <i>'Find all periods where a city experienced 3 or more consecutive readings of Unhealthy AQI (>150).'</i><br/>"
        "<b>The Trick:</b> Calculate two independent row numbers over chronological order:",
        body_style
    ))

    sql_walkthrough = (
        "Reading TS  | AQI | Is Unhealthy? | rn (All rows) | rn2 (Only Unhealthy) | Group Key (rn - rn2)\n"
        "10:00 AM    |  80 | False         | 1             | -                    | -\n"
        "01:00 PM    | 160 | True          | 2             | 1                    | 2 - 1 = 1  <-- Island 1\n"
        "04:00 PM    | 175 | True          | 3             | 2                    | 3 - 2 = 1  <-- Island 1\n"
        "07:00 PM    | 190 | True          | 4             | 3                    | 4 - 3 = 1  <-- Island 1 (Length 3!)\n"
        "10:00 PM    |  95 | False         | 5             | -                    | - (Streak Broken)\n"
        "01:00 AM    | 210 | True          | 6             | 4                    | 6 - 4 = 2  <-- Island 2"
    )
    story.append(code_box(sql_walkthrough))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Why this works:</b> As long as readings stay unhealthy, both `rn` and `rn2` increment by +1, keeping the difference `(rn - rn2)` constant. "
        "The moment a healthy reading occurs, `rn` increments but `rn2` does not, causing `(rn - rn2)` to shift to a new integer. Grouping by `(city_id, island_id)` "
        "allows you to run `COUNT(*) >= 3` and calculate `MIN(reading_ts)` and `MAX(reading_ts)` for streak duration.",
        body_style
    ))

    story.append(Paragraph("Problem 2: Rolling 24-Hour Average with `RANGE` Frame", h2_style))
    story.append(Paragraph(
        "<b>Interview Question:</b> <i>'Calculate the 24-hour rolling average temperature per city. Why use RANGE instead of ROWS?'</i>",
        body_style
    ))
    code_range = (
        "SELECT\n"
        "    city_id, reading_ts, temperature_c,\n"
        "    ROUND(AVG(temperature_c) OVER (\n"
        "        PARTITION BY city_id\n"
        "        ORDER BY reading_ts\n"
        "        RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW\n"
        "    )::NUMERIC, 2) AS rolling_avg_24h\n"
        "FROM fact_weather_reading;"
    )
    story.append(code_box(code_range))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "<b>Your Defense:</b> <code>ROWS BETWEEN 7 PRECEDING AND CURRENT ROW</code> assumes exactly 8 observations at fixed 3-hour intervals. "
        "If a sensor was offline for 12 hours, a <code>ROWS</code> frame reaches back into observations from 36 hours ago to collect 8 rows, producing an inaccurate rolling average. "
        "<code>RANGE BETWEEN INTERVAL '24 hours' PRECEDING</code> operates on the <i>value</i> of the timestamp, including only rows whose timestamp is within the actual 24-hour clock window.",
        body_style
    ))

    story.append(Paragraph("Problem 3: Ranking Functions Differences (`RANK` vs `DENSE_RANK` vs `ROW_NUMBER`)", h2_style))
    story.append(Paragraph(
        "<b>Interview Question:</b> <i>'How did you build the weekly city AQI leaderboard, and why use RANK()?'</i><br/>"
        "&bull; <b>`ROW_NUMBER()`:</b> Arbitrarily breaks ties (1, 2, 3, 4). Inappropriate for a fair pollution ranking.<br/>"
        "&bull; <b>`DENSE_RANK()`:</b> Tied cities share rank, next rank is sequential (1, 2, 2, 3).<br/>"
        "&bull; <b>`RANK()`:</b> Tied cities share rank, next rank skips by the number of ties (1, 2, 2, 4). We use `RANK()` because if two cities tie for 2nd place, the next city is legitimately the 4th most polluted city.",
        body_style
    ))

    story.append(Spacer(1, 10))

    # =========================================================================
    # SECTION 6: WHAT TO AVOID SAYING (INTERVIEW RED FLAGS)
    # =========================================================================
    story.append(PageBreak())
    story.append(Paragraph("6. Common Interview Traps & What NEVER to Say", h1_style))
    traps = [
        ("Never say: 'I used Airflow XComs to pass all the weather data between tasks.'",
         "Why it hurts: XComs are stored in the Airflow metadata database (Postgres/MySQL) as BLOBs/strings. Passing megabytes of data bloats the metadata DB and causes scheduler latency. "
         "Say instead: 'I used XComs only for lightweight task metadata (counts, run IDs, status flags). All heavy payloads were staged in MongoDB and retrieved by downstream workers.'"),
        ("Never say: 'The pipeline never fails because I put everything in a try-except block.'",
         "Why it hurts: Silent exception swallowing is a critical bug in production data pipelines. "
         "Say instead: 'I caught specific network exceptions to handle partial failure gracefully, but logged errors to pipeline_run_log and raised explicit RuntimeErrors in the data quality gate to fail the task if data integrity was violated.'"),
        ("Never say: 'I just truncated and reloaded the tables on every run.'",
         "Why it hurts: Truncate-and-load destroys historical trend analytics, prevents point-in-time rollups, and locks tables during reload. "
         "Say instead: 'I implemented an idempotent upsert pattern using PostgreSQL ON CONFLICT (city_id, reading_ts) DO UPDATE, preserving historical facts while permitting safe re-runs.'"),
        ("Never say: 'I tested the pipeline by watching it run in the Airflow UI.'",
         "Why it hurts: Lacks software engineering maturity. "
         "Say instead: 'I developed an automated test suite of 77 pytest unit tests with mock cursors and JSON payloads for offline validation, plus a DagBag integrity test catching import/syntax bugs before deployment.'")
    ]
    for bad, good in traps:
        t_trap = Table([
            [Paragraph(f"<b>RED FLAG:</b> {bad}", ParagraphStyle('RF', parent=body_style, textColor=colors.HexColor("#C53030")))],
            [Paragraph(f"<b>STRONG ANSWER:</b> {good}", ParagraphStyle('SA', parent=body_style, textColor=colors.HexColor("#2F855A")))]
        ], colWidths=[letter[0] - 108])
        t_trap.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor("#F7FAFC")),
            ('BOX', (0, 0), (-1, -1), 0.5, box_border),
            ('PADDING', (0, 0), (-1, -1), 4.5),
        ]))
        story.append(t_trap)
        story.append(Spacer(1, 4))

    # =========================================================================
    # SECTION 7: INTERVIEW CHEATSHEET & KEY METRICS SUMMARY
    # =========================================================================
    story.append(Spacer(1, 6))
    story.append(Paragraph("7. Interview Day Quick-Fire Metrics Cheatsheet", h1_style))
    story.append(Paragraph("Memorize these hard numbers and architectural primitives before your interview:", body_style))

    metrics_data = [
        [Paragraph("<b>Metric / Primitive</b>", h3_style), Paragraph("<b>Specification in This Project</b>", h3_style), Paragraph("<b>Interview Talking Value</b>", h3_style)],
        [Paragraph("Active Target Cities", body_style), Paragraph("34 canonical Indian metropolitan cities", body_style), Paragraph("Demonstrates real multi-entity geographical tracking", body_style)],
        [Paragraph("Pipeline Cadence", body_style), Paragraph("Every 3 hours (0 */3 * * *) = 8 runs/day", body_style), Paragraph("Trivial against API rate limits while keeping data fresh", body_style)],
        [Paragraph("Open-Meteo Optimization", body_style), Paragraph("Batched into 4 HTTP requests (10 cities/batch)", body_style), Paragraph("Shows awareness of socket overhead and URL limits", body_style)],
        [Paragraph("WAQI Extraction", body_style), Paragraph("34 dynamically mapped tasks (.expand())", body_style), Paragraph("Isolates per-station timeouts and failure blast radius", body_style)],
        [Paragraph("Data Quality Gate", body_style), Paragraph("10 automated checks (schema, range, nulls, freshness)", body_style), Paragraph("Fail-fast enforcement + audit persistence in single txn", body_style)],
        [Paragraph("Offline Test Suite", body_style), Paragraph("77 unit tests passing in < 2 seconds", body_style), Paragraph("Demonstrates test-driven development (TDD) discipline", body_style)],
        [Paragraph("Idempotency Key", body_style), Paragraph("(city_id, reading_ts) compound unique constraint", body_style), Paragraph("Guarantees re-runs and backfills never duplicate rows", body_style)],
    ]
    t_metrics = Table(metrics_data, colWidths=[120, 190, letter[0] - 108 - 310])
    t_metrics.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#EDF2F7")),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor("#CBD5E0")),
        ('PADDING', (0, 0), (-1, -1), 4),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    story.append(t_metrics)

    # Build the document
    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"Successfully generated: {filename}")


if __name__ == "__main__":
    out_path = Path("Interview_Preparation_Guide.pdf").resolve()
    build_pdf(str(out_path))
