# Deployment Guide for MomentumChaser.com

This guide covers deploying the enhanced kite-swing-scan application with database storage and interactive features.

## 🏗️ Architecture Overview

The application now has multiple components that need to be deployed:

```
Frontend (Static)     API Server (Dynamic)     Database        Scheduler
site/index.html  →   src/api_server.py    →   SQLite DB   ←   Cron Jobs
     ↓                      ↓                     ↓              ↓
Static Hosting       Python Backend        File Storage    Daily Scans
(Vercel/Netlify)    (VPS/Cloud/Railway)   (Persistent)    (GitHub Actions)
```

## 🚀 Deployment Options

### Option 1: Full Stack on VPS (Recommended)
**Best for**: Complete control, database persistence, scheduled scans

**Requirements:**
- VPS with Python 3.8+ (DigitalOcean, Linode, AWS EC2)
- Domain pointing to your server
- SSL certificate (Let's Encrypt)

**Setup:**
```bash
# On your VPS
git clone <your-repo-url>
cd kite-swing-scan

# Install dependencies
pip install -r requirements.txt  # You'll need to create this

# Setup environment
cp .env.example .env  # Add your Kite API credentials

# Setup systemd services
sudo cp deploy/kite-scanner.service /etc/systemd/system/
sudo cp deploy/api-server.service /etc/systemd/system/
sudo systemctl enable kite-scanner api-server
sudo systemctl start api-server

# Setup nginx reverse proxy
sudo cp deploy/nginx.conf /etc/nginx/sites-available/momentumchaser.com
sudo ln -s /etc/nginx/sites-available/momentumchaser.com /etc/nginx/sites-enabled/
sudo systemctl reload nginx

# Setup daily scan cron job
echo "0 18 * * 1-5 cd /path/to/kite-swing-scan && python src/scan.py && python src/publish_site.py" | crontab -
```

### Option 2: Hybrid (Static + Serverless)
**Best for**: Cost efficiency, auto-scaling

**Frontend**: Deploy static files to Vercel/Netlify
**Backend**: Deploy API to Railway/Render/Heroku
**Database**: Use managed PostgreSQL instead of SQLite

### Option 3: GitHub Actions + Static Hosting
**Best for**: Simple setup, no server maintenance

**Approach**: Use GitHub Actions to run scans, commit results to repo, deploy static site

## 📋 Deployment Checklist

### Pre-deployment Setup

1. **Create requirements.txt**
```bash
pip freeze > requirements.txt
```

2. **Environment Configuration**
```bash
# Create .env.example (without secrets)
cp .env .env.example
# Remove actual API keys, keep structure
```

3. **Database Migration** (if using PostgreSQL)
```python
# Modify src/database.py to use PostgreSQL instead of SQLite
# Add connection string from environment
```

### Production Configuration

1. **Security**
```python
# In src/api_server.py, update for production:
app.run(host='0.0.0.0', port=8000, debug=False)

# Add rate limiting, API keys if needed
```

2. **CORS Configuration**
```python
# Update CORS settings for your domain
CORS(app, origins=['https://momentumchaser.com'])
```

3. **Database Persistence**
```bash
# Ensure database directory is writable
chmod 755 data/
```

## 🔄 Automated Deployment Scripts

### GitHub Actions Workflow
Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy MomentumChaser

on:
  schedule:
    - cron: '0 18 * * 1-5'  # 6 PM IST on weekdays
  workflow_dispatch:

jobs:
  scan-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: pip install -r requirements.txt
        
      - name: Setup environment
        env:
          KITE_API_KEY: \${{ secrets.KITE_API_KEY }}
          KITE_API_SECRET: \${{ secrets.KITE_API_SECRET }}
        run: |
          echo "KITE_API_KEY=\$KITE_API_KEY" > .env
          echo "KITE_API_SECRET=\$KITE_API_SECRET" >> .env
          
      - name: Authenticate with Kite (if needed)
        # You'll need to handle this - tokens expire daily
        
      - name: Run scan
        run: |
          python src/scan.py
          python src/publish_site.py
          
      - name: Deploy to hosting
        # Deploy site/ directory to your hosting provider
```

### Railway Deployment
Create `railway.toml`:

```toml
[build]
builder = "NIXPACKS"

[deploy]
startCommand = "python src/api_server.py"

[env]
PORT = "8000"
```

## 🌐 Domain & DNS Setup

1. **Point domain to your server**
```
A record: momentumchaser.com → YOUR_SERVER_IP
CNAME: www.momentumchaser.com → momentumchaser.com
```

2. **SSL Certificate**
```bash
sudo certbot --nginx -d momentumchaser.com -d www.momentumchaser.com
```

## 📊 Monitoring & Maintenance

### Health Checks
- API endpoint: `https://momentumchaser.com/api/health`
- Database size monitoring
- Scan success/failure notifications

### Backup Strategy
```bash
# Daily database backup
0 2 * * * cp /path/to/data/stock_data.db /backups/stock_data_$(date +%Y%m%d).db
```

### Log Management
```bash
# Setup log rotation for scan logs
sudo logrotate -d /etc/logrotate.d/kite-scanner
```

## 🚨 Critical Notes

1. **Kite Connect Authentication**: Tokens expire daily and require manual refresh
2. **Rate Limits**: Respect Kite Connect API limits (3 requests/second)
3. **Market Hours**: Schedule scans after market closes (post 3:30 PM IST)
4. **Database Growth**: Monitor database size - it will grow over time
5. **Error Handling**: Set up alerts for scan failures

## 🔧 Deployment Commands Summary

```bash
# Push to git (already done)
git push origin master

# For VPS deployment:
ssh your-server
git pull origin master
sudo systemctl restart api-server
sudo systemctl reload nginx

# For static deployment:
# Upload site/ directory to your hosting provider
```

---

**Next Steps:**
1. Choose your deployment option
2. Set up the infrastructure
3. Configure domain and SSL
4. Test the deployment
5. Set up monitoring and backups

**Need help with deployment?** The specific steps will depend on your chosen hosting provider and infrastructure setup.