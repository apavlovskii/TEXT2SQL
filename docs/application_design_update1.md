 Make few modifications in the application.
 1. Instead of displaying just "Running agent pipeline..." make this message more granular and updated with actual stage of what the agent is doing at the moment. For example: retrieving schema from RAG, generating query version 1, calling LLM, etc. Chose granularity yourself, but it should be more frequently updating message.
 2. Allow the user to chose the model (GPT-5.4, GPT-5-mini, GPT-5-nano, GPT-4o) and parameters like "Max retries" (default 10), "Max candidates" (default 2), 
 3. The query generation should repeat until it comes up with at least compilable query. There must be no error coming out to the user. If it's not able to compile after Max retries output message "Unable to generate valid query".
 4. There must be a button that terminates agent execution.
 5. Instead of simple database selector give a drill down control that allows to select database, then check its tables and columns - aka schema browser.
 6. Allow user to chose datasource: SQLite(default) or snowflake.
 7. Add a panel that will show detailed execution log for debugging purposes.
 8. Browser window should be called "Analytics insite" instead of "Frontend".
 9. Most important, if a question is not related to data in database just use raw model to provide the answer without looking into rag and using the agent.
 10. make execution log a collapsible vertical panel on the right, that can slide out to 20% of the screen width and slide back. 
 11. Display result panel first before sql query, result should be uncollapsed initially. 
 12. Also add a collapsed element to display collection descriptions of the vector DB, by default collapsed but can be inspected by the user if required. It should be in display only mode.