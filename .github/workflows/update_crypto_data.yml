# .github/workflows/update_crypto_data.yml
name: Update Crypto Drawdown Data

on:
  schedule:
    - cron: '0 */2 * * *' # Every 2 hours
  workflow_dispatch: # Allows manual trigger from GitHub UI

jobs:
  update_data:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.x'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install requests boto3

    - name: Run data uploader script
      env:
        R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
        R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
        R2_ENDPOINT_URL: ${{ secrets.R2_ENDPOINT_URL }}
        R2_BUCKET_NAME: ${{ secrets.R2_BUCKET_NAME }}
      run: python scripts/crypto_drawdown_uploader.py # Ubah nama skrip di sini
