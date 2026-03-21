SELECT "repo_name", "commit_count" FROM (SELECT l."repo_name", COUNT(s."commit") AS "commit_count" 
FROM "GITHUB_REPOS"."LANGUAGES" l 
JOIN "GITHUB_REPOS"."SAMPLE_COMMITS" s ON l."repo_name" = s."repo_name" 
WHERE l."language" ILIKE '%JavaScript%' 
GROUP BY l."repo_name" 
ORDER BY "commit_count" DESC 
LIMIT 2) AS top_repos;
