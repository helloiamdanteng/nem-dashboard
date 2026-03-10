# NEM Dashboard

A mobile-friendly dashboard for Australian electricity market data, scraping live data from AEMO's NEMWeb.

## Features
- ⚡ **Spot Prices** — Real-time RRP for all 5 NEM regions (QLD, NSW, VIC, SA, TAS)
- 🔋 **Generation** — Dispatched and semi-scheduled generation by region
- 📊 **Demand** — Total demand, forecast, and initial supply
- 🔗 **Interconnectors** — Live MW flows between regions
- 📱 **Mobile-first** — Designed for iPhone, works great in Safari

## Data Source
All data is fetched from [AEMO NEMWeb](https://www.nemweb.com.au) — specifically the `PUBLIC_DISPATCHIS` reports, updated every 5 minutes.

---

## Deploy to Render + GitHub

### 1. Push to GitHub

```bash
git init
git add .
git commit -m "Initial NEM dashboard"
gh repo create nem-dashboard --public --push
```

### 2. Deploy on Render

1. Go to [render.com](https://render.com) and sign in
2. Click **New → Web Service**
3. Connect your GitHub repo
4. Render will auto-detect `render.yaml` and configure everything
5. Click **Deploy**

Your dashboard will be live at `https://nem-dashboard.onrender.com` (or similar).

### 3. Access on iPhone

Open Safari on your iPhone and navigate to the Render URL.  
To add to home screen: tap **Share → Add to Home Screen** for a native app feel.

---

## Local Development

```bash
pip install -r requirements.txt
python main.py
# Open http://localhost:8000
```

## File Structure

```
nem-dashboard/
├── main.py          # FastAPI app — serves data API + dashboard
├── scraper.py       # NEMWeb scraper (prices, demand, generation, interconnectors)
├── requirements.txt
├── render.yaml      # Render deployment config
└── static/
    └── index.html   # Mobile-friendly dashboard UI
```

## Notes

- Data refreshes automatically every **5 minutes** (matching NEM dispatch interval)
- The Render free tier spins down after inactivity — first load may take ~30s to wake
- For always-on, upgrade to Render Starter ($7/mo) or use a paid plan
