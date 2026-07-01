# Start and Use the EUDR Portal in a Browser

This guide is for operators and reviewers who need to open the private EUDR client portal, sign in, view reports, and create a new report from browser inputs.

The browser portal lives in the sibling repository:

```sh
../eudr-client-portal
```

This repository, `eudr-dmi-gil`, remains the authoritative report generator. The portal delegates report generation here and stores private run artifacts through its own database and object-storage configuration.

## 1. Start the Portal

### Option A: local development server

Use this when you are working from the source checkout.

```sh
cd ../eudr-client-portal
npm install
npm run dev
```

Open:

```text
http://localhost:3000
```

If this is a first-time local setup, prepare the portal environment from its own examples before starting:

- `../eudr-client-portal/.env.example`
- `../eudr-client-portal/docs/operations/dev_runbook.md`

At minimum, the portal needs:

- `DATABASE_URL`
- `NEXTAUTH_SECRET`
- `APP_BASE_URL` or `NEXTAUTH_URL`
- object storage variables such as `MINIO_ENDPOINT`, `MINIO_BUCKET_PRIVATE`, `MINIO_ACCESS_KEY`, and `MINIO_SECRET_KEY`
- `EUDR_DMI_GIL_REPO`, normally `../eudr-dmi-gil`
- `EUDR_DMI_GIL_INTERFACE_CMD`, normally `python -m eudr_dmi_gil.reports.cli`

If the local database is new, generate Prisma client code and apply migrations from the portal repo:

```sh
npx prisma generate
npx prisma migrate deploy
```

### Option B: local Docker Compose stack

Use this when you want the deployed-style stack with Postgres, Mailpit, local LLM services, and the portal container.

```sh
cd ../eudr-client-portal
docker compose up -d
```

Open:

```text
http://localhost:3000
```

For local email verification in this stack, open Mailpit:

```text
http://localhost:8025
```

The Compose stack requires secrets in the portal environment, including `NEXTAUTH_SECRET`, `AUTH_CREDENTIALS_PASSWORD`, MinIO credentials, and `LLM_SERVICE_SECRET`. Do not commit real credentials.

## 2. Register and Log In

### Existing account

1. Open `http://localhost:3000/login`.
2. Enter email and password.
3. Click `Sign in`.
4. The portal redirects you to:
   - `/onboarding` if onboarding is incomplete.
   - `/dashboard` if onboarding is complete.

### New account

1. Open `http://localhost:3000/register`.
2. Enter an email address.
3. Optionally enter a company name.
4. Click `Register`.
5. Open the verification email:
   - In local Docker/dev Mailpit: `http://localhost:8025`.
   - In production/staging: the user's real inbox.
6. Click the latest verification link.
7. On `Set Password`, enter a password of at least 12 characters and confirm it.
8. After password setup, continue to the dashboard or sign in at `/login`.

## 3. View Public and Sample Reports

You can view public/demo material before or after login.

From the dashboard header:

- `Sample Reports` opens the portal landing page with downloadable example reports.
- `DAO Bundles` opens the public Digital Twin AOI bundle index in a new tab.
- `AOI Editor` opens the editor for drawing or importing areas of interest.
- `About EUDR` appears on the public dashboard and links to the public DTE/EUDR guidance surface.

Use public/sample reports for inspection and demos only. Private client AOI reports are not published to the Digital Twin by default.

## 4. Complete Onboarding Before Creating Reports

Open:

```text
http://localhost:3000/onboarding
```

The portal has four onboarding cards.

### Step 1: Upload KYC

1. Use `.docx` when possible. `.txt` is also accepted.
2. If you only have a Google Docs `.gdoc` pointer, export it first: Google Docs -> File -> Download -> Microsoft Word (`.docx`).
3. Click `Upload KYC and review`.
4. On `KYC review`, check and edit the company fields.
5. `Legal company name` and `Country of establishment` are required.
6. Click `Save`.
7. After `KYC saved successfully`, follow the link back to onboarding.

For demos, the onboarding page includes `Use sample KYC`.

### Step 2: Data Availability

1. Click `Start assessment`.
2. Answer the questions about whether you already have plot data.
3. Select the data type and format when applicable.
4. Select the number of plots.
5. Click `Confirm next step`.
6. Use the recommendation to proceed to the AOI flow.

This step must be complete before AOI upload is enabled.

### Step 3: Upload AOI

1. Upload a GeoJSON file with Polygon or MultiPolygon features.
2. Accepted extensions are `.geojson` and `.json`.
3. If there are multiple companies, choose the company for the AOI before uploading.
4. Click `Upload AOI`.

For demos, the onboarding page includes `Use sample AOI`, which uploads `estonia_testland1.geojson`.

### Step 4: Review

When KYC, data availability, and AOI are complete, click:

```text
Continue to dashboard
```

## 5. Create a Report

1. Open `/dashboard`.
2. Confirm the dashboard shows `Onboarding complete`.
3. In `EUDR Deforestation Screening`, click `Generate Report`.
4. The portal starts a run through `POST /api/runs/start`.
5. The browser redirects to:

```text
/runs/<runId>
```

Run status progresses through:

```text
queued -> running -> succeeded
```

If status becomes `failed`, read the error message in `Run Metadata`. Common causes are missing AOI data, object-storage configuration errors, database connectivity problems, or an unavailable `eudr-dmi-gil` generator environment.

## 6. View and Download a Generated Report

On `/runs/<runId>`, wait until the run has produced structured report data.

Use `Report Actions`:

- `Open HTML report` opens the generated browser report.
- `Create PDF Report` downloads a PDF generated from the same report selection.
- `View bundle_manifest.json` opens the verified bundle manifest.
- `Open Inspector Chat` opens a run-scoped chat view for questions about that run.

Use `Fields` to choose which report tables and fields appear in the generated HTML/PDF report.

Use `Map Layers` to choose the evidence layers included in the static report map. The page shows the maximum number of layers that can be selected.

If the legacy run details page is enabled through `LEGACY_RUN_DETAILS_UI=1`, the page instead shows:

- `Open HTML report`
- `Download JSON report`
- `View bundle_manifest.json`
- an `Artifacts` list with proxied links to stored run files
- an Inspector Chat panel with an `Attach report context` checkbox

## 7. Useful Browser URLs

```text
/dashboard
/login
/register
/onboarding
/onboarding/data-availability
/aoi/editor
/landing
/runs/<runId>
/runs/<runId>/report/html
/runs/<runId>/report/pdf
/runs/<runId>/report/manifest
```

Local service URLs:

```text
Portal:  http://localhost:3000
Mailpit: http://localhost:8025
Grafana: http://localhost:3001
```

## 8. Operational Notes

- Private report artifacts must remain in private portal storage.
- Public Digital Twin reports are examples and inspection aids, not authoritative private client records.
- Do not commit `.env` files or credentials.
- Use `../eudr-client-portal/docs/ui/aoi_run_workflow.md` for the portal-side UI workflow.
- Use `docs/reports/runbook_generate_aoi_report.md` in this repo for the generator-side AOI report workflow.
