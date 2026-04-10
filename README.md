name: Refresh Cash Card Dashboard

on:
  schedule:
    # Run every hour at :05 past the hour
    - cron: '5 * * * *'
  
  # Allow manual trigger from GitHub UI
  workflow_dispatch:

jobs:
  refresh:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install snowflake-connector-python requests
      
      - name: Run dashboard refresh
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
        run: |
          python scripts/refresh_dashboard.py
      
      - name: Commit and push if changed
        run: |
          git config --global user.name 'Dashboard Bot'
          git config --global user.email 'dashboard-bot@users.noreply.github.com'
          git add index.html
          git diff --quiet && git diff --staged --quiet || (git commit -m "🔄 Auto-update dashboard $(date -u +'%Y-%m-%d %H:%M:%S UTC')" && git push)
