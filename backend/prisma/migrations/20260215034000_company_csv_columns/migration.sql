ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_name_for_emails TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS employees TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS website TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_linkedin_url TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS facebook_url TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS twitter_url TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_city TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_state TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_country TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS keywords TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_phone TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS total_funding TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS latest_funding TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS latest_funding_amount TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_raised_at TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS annual_revenue TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS apollo_account_id TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS sic_codes TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS naics_codes TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS short_description TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS founded_year TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS subsidiary_of TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS stablecoin_prospect_9854 TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS qualify_account TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS prerequisite_determine_research_guidelines TEXT NOT NULL DEFAULT '';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS prerequisite_research_target_company TEXT NOT NULL DEFAULT '';

UPDATE companies
SET
  company_name_for_emails = COALESCE(NULLIF(company_name_for_emails, ''), COALESCE(profile_json::jsonb ->> 'company_name_for_emails', profile_json::jsonb ->> 'Company Name for Emails', '')),
  employees = COALESCE(NULLIF(employees, ''), COALESCE(profile_json::jsonb ->> 'employees', profile_json::jsonb ->> '# Employees', '')),
  industry = COALESCE(NULLIF(industry, ''), COALESCE(profile_json::jsonb ->> 'industry', profile_json::jsonb ->> 'Industry', '')),
  website = COALESCE(NULLIF(website, ''), COALESCE(profile_json::jsonb ->> 'website', profile_json::jsonb ->> 'Website', '')),
  company_linkedin_url = COALESCE(NULLIF(company_linkedin_url, ''), COALESCE(profile_json::jsonb ->> 'company_linkedin_url', profile_json::jsonb ->> 'Company Linkedin Url', '')),
  facebook_url = COALESCE(NULLIF(facebook_url, ''), COALESCE(profile_json::jsonb ->> 'facebook_url', profile_json::jsonb ->> 'Facebook Url', '')),
  twitter_url = COALESCE(NULLIF(twitter_url, ''), COALESCE(profile_json::jsonb ->> 'twitter_url', profile_json::jsonb ->> 'Twitter Url', '')),
  company_city = COALESCE(NULLIF(company_city, ''), COALESCE(profile_json::jsonb ->> 'company_city', profile_json::jsonb ->> 'Company City', '')),
  company_state = COALESCE(NULLIF(company_state, ''), COALESCE(profile_json::jsonb ->> 'company_state', profile_json::jsonb ->> 'Company State', '')),
  company_country = COALESCE(NULLIF(company_country, ''), COALESCE(profile_json::jsonb ->> 'company_country', profile_json::jsonb ->> 'Company Country', '')),
  keywords = COALESCE(NULLIF(keywords, ''), COALESCE(profile_json::jsonb ->> 'keywords', profile_json::jsonb ->> 'Keywords', '')),
  company_phone = COALESCE(NULLIF(company_phone, ''), COALESCE(profile_json::jsonb ->> 'company_phone', profile_json::jsonb ->> 'Company Phone', '')),
  total_funding = COALESCE(NULLIF(total_funding, ''), COALESCE(profile_json::jsonb ->> 'total_funding', profile_json::jsonb ->> 'Total Funding', '')),
  latest_funding = COALESCE(NULLIF(latest_funding, ''), COALESCE(profile_json::jsonb ->> 'latest_funding', profile_json::jsonb ->> 'Latest Funding', '')),
  latest_funding_amount = COALESCE(NULLIF(latest_funding_amount, ''), COALESCE(profile_json::jsonb ->> 'latest_funding_amount', profile_json::jsonb ->> 'Latest Funding Amount', '')),
  last_raised_at = COALESCE(NULLIF(last_raised_at, ''), COALESCE(profile_json::jsonb ->> 'last_raised_at', profile_json::jsonb ->> 'Last Raised At', '')),
  annual_revenue = COALESCE(NULLIF(annual_revenue, ''), COALESCE(profile_json::jsonb ->> 'annual_revenue', profile_json::jsonb ->> 'Annual Revenue', '')),
  apollo_account_id = COALESCE(NULLIF(apollo_account_id, ''), COALESCE(profile_json::jsonb ->> 'apollo_account_id', profile_json::jsonb ->> 'Apollo Account Id', '')),
  sic_codes = COALESCE(NULLIF(sic_codes, ''), COALESCE(profile_json::jsonb ->> 'sic_codes', profile_json::jsonb ->> 'SIC Codes', '')),
  naics_codes = COALESCE(NULLIF(naics_codes, ''), COALESCE(profile_json::jsonb ->> 'naics_codes', profile_json::jsonb ->> 'NAICS Codes', '')),
  short_description = COALESCE(NULLIF(short_description, ''), COALESCE(profile_json::jsonb ->> 'short_description', profile_json::jsonb ->> 'Short Description', '')),
  founded_year = COALESCE(NULLIF(founded_year, ''), COALESCE(profile_json::jsonb ->> 'founded_year', profile_json::jsonb ->> 'Founded Year', '')),
  subsidiary_of = COALESCE(NULLIF(subsidiary_of, ''), COALESCE(profile_json::jsonb ->> 'subsidiary_of', profile_json::jsonb ->> 'Subsidiary of', '')),
  stablecoin_prospect_9854 = COALESCE(NULLIF(stablecoin_prospect_9854, ''), COALESCE(profile_json::jsonb ->> 'stablecoin_prospect_9854', profile_json::jsonb ->> 'Stablecoin Prospect 9854', '')),
  qualify_account = COALESCE(NULLIF(qualify_account, ''), COALESCE(profile_json::jsonb ->> 'qualify_account', profile_json::jsonb ->> 'Qualify Account', '')),
  prerequisite_determine_research_guidelines = COALESCE(NULLIF(prerequisite_determine_research_guidelines, ''), COALESCE(profile_json::jsonb ->> 'prerequisite_determine_research_guidelines', profile_json::jsonb ->> 'Prerequisite: Determine Research Guidelines', '')),
  prerequisite_research_target_company = COALESCE(NULLIF(prerequisite_research_target_company, ''), COALESCE(profile_json::jsonb ->> 'prerequisite_research_target_company', profile_json::jsonb ->> 'Prerequisite: Research Target Company', ''))
WHERE profile_json IS NOT NULL AND profile_json <> '';

ALTER TABLE companies DROP COLUMN IF EXISTS profile_json;
