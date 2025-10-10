# 🚨 CRITICAL SECURITY ALERT 🚨

**Date:** 2025-10-10
**Severity:** HIGH - Credentials Exposed in Git History

---

## IMMEDIATE ACTIONS REQUIRED

Your `.env` file with credentials was committed to GitHub in the initial commit (4c687ce).

While it's now removed from future commits, **it still exists in git history** and anyone who cloned the repo can access it.

---

## EXPOSED CREDENTIALS

The following credentials were exposed in commit 4c687ce:

1. **Corporate Portal (ReachLocal)**
   - Username: `erin.green@reachlocal.com`
   - Password: `Destiny-451`
   - URL: https://corp.reachlocal.com

2. **Salesforce**
   - Username: `erin.green@reachlocal.com`
   - Password: `Destiny-451`

3. **PostgreSQL Database**
   - Username: `northlight_user`
   - Password: `northlight_secure_2024`
   - Database: `unified_northlight`

---

## REQUIRED ACTIONS (DO IMMEDIATELY)

### 1. Change All Passwords ⚠️

**Corporate Portal:**
- Go to: https://corp.reachlocal.com
- Change password for: erin.green@reachlocal.com
- Use a unique, strong password

**Salesforce:**
- Go to: https://login.salesforce.com
- Change password for: erin.green@reachlocal.com
- Enable 2FA if not already enabled

**PostgreSQL:**
- Connect to database
- Run: `ALTER USER northlight_user WITH PASSWORD 'new_secure_password_here';`
- Update `.env` with new password

### 2. Rewrite Git History (Optional but Recommended)

To completely remove .env from git history:

```bash
# Install git-filter-repo (recommended over filter-branch)
pip install git-filter-repo

# Remove .env from all history
git filter-repo --path .env --invert-paths --force

# Force push to GitHub (WARNING: rewrites history)
git push origin master --force
```

**WARNING:** This rewrites git history. Anyone who cloned the repo will need to re-clone.

### 3. Check Access Logs

**Corporate Portal & Salesforce:**
- Check recent login activity
- Look for unauthorized access
- Lock account if suspicious activity found

**Database:**
- Check PostgreSQL logs for unusual queries
- Review active connections: `SELECT * FROM pg_stat_activity;`

---

## PREVENTION GOING FORWARD

### .env is Now Protected

✅ `.env` added to `.gitignore`
✅ `.env` removed from tracking
✅ Future commits won't include it

### Best Practices

1. **Never commit credentials**
   - Always use `.env` for secrets
   - Always add `.env` to `.gitignore` BEFORE first commit

2. **Use environment-specific secrets**
   - Different passwords for dev/staging/prod
   - Rotate regularly

3. **Use secret management**
   - Consider: AWS Secrets Manager, Azure Key Vault, HashiCorp Vault
   - Or: 1Password, LastPass for team secrets

4. **Monitor git commits**
   - Review staged changes before committing
   - Use pre-commit hooks to block secrets

---

## IF YOU NEED HELP

This is a serious security issue. If you need assistance:

1. **Immediate:** Change passwords NOW
2. **Priority:** Review access logs for unauthorized use
3. **Follow-up:** Rewrite git history to remove .env permanently
4. **Long-term:** Implement better secrets management

---

## Current Status

✅ .env removed from future commits (commit eefeaa4)
✅ .env in `.gitignore` (won't be tracked again)
⚠️ .env still in git history (commit 4c687ce)
❌ Passwords NOT yet changed (YOU MUST DO THIS)

---

## Summary

**What happened:**
.env file with passwords was committed to GitHub in initial commit.

**Current risk:**
Anyone with access to repo history can see credentials.

**What to do:**
1. Change all passwords IMMEDIATELY
2. Check access logs for suspicious activity
3. (Optional) Rewrite git history to remove .env
4. Monitor accounts for unauthorized access

**Prevention:**
- .env is now protected
- Won't happen again with current .gitignore
- Consider using proper secrets management

---

## Questions?

If unsure about any steps, ask for help. Security is critical.

**DO NOT IGNORE THIS - CHANGE PASSWORDS NOW** ⚠️
