# Enrichment Scripts

This directory contains scripts for enriching property listing data, primarily using Compass.com.

## Authentication with Compass.com

The enrichment scripts (`enrich_compass_to_json.py`, `enrich_with_compass.py`, `enrich_with_compass_details.py`) use Playwright to interact with Compass.com and require authentication. These scripts utilize a persistent browser context, meaning you only need to manually authenticate once.

### First-Time Authentication Setup

To ensure the scripts can run in headless mode (e.g., on a server or for automated tasks), you must first establish an authenticated session. Follow these steps:

1.  **Run an enrichment script in headed mode:**
    Open your terminal and navigate to the project's root directory.
    Execute one of the enrichment scripts **without** the `--headless` flag. For example:
    ```bash
    python scripts/enrich_compass_to_json.py --limit 1
    ```
    Or, for `enrich_with_compass_details.py` (which also now supports headless but needs initial headed run):
    ```bash
    python scripts/enrich_with_compass_details.py --limit 1
    ```
    Using `--limit 1` is recommended for this initial setup to process only one listing.

2.  **Log in to Compass.com:**
    A browser window will open. The script will navigate to Compass.com. If you are not already logged in, the script will pause and prompt you to log in. Please complete the login process within this browser window (e.g., using your Google account or other Compass login credentials).

3.  **Confirm Authentication:**
    Once you have successfully logged in, the script will detect this, save the authentication state (cookies, local storage, etc.) to the `.auth/compass` directory in your project root, and then proceed with its task (enriching the one listing in the example above).

4.  **Ready for Headless Operation:**
    After the script completes successfully, your authentication details are stored. You can now run any of an_enrichment_scripts with the `--headless` flag, and they will use the saved session.

    Example of running headlessly:
    ```bash
    python scripts/enrich_compass_to_json.py --headless
    python scripts/enrich_with_compass.py --headless
    python scripts/enrich_with_compass_details.py --headless
    ```

### Authentication Notes

*   **Session Validity:** The saved authentication session should remain valid for a considerable period, but Compass.com might invalidate it after some time (e.g., weeks or months). If you encounter authentication errors in headless mode after a while, repeat the steps above to re-authenticate in headed mode.
*   **Security:** The `.auth/compass` directory contains sensitive session information. Ensure this directory is included in your `.gitignore` file (it should be by default if you're using a standard Python gitignore) and is not committed to your repository.
*   **Multiple Environments:** If you run these scripts in different environments (e.g., local machine and a server), you will need to perform this first-time authentication setup in each environment where the scripts will run.
