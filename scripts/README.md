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

### Using on a Headless Server

If you need to run these scripts on a server that does not have a graphical display environment (a "headless" server), you cannot perform the initial graphical login directly on that server. Here's the recommended workflow:

1.  **Generate Session Locally:** Perform the "First-Time Authentication Setup" (steps 1-3 described above) on your local machine or any machine *with* a graphical display. This will create and populate the `.auth/compass` directory in your project's root with the necessary authentication tokens.

2.  **Securely Transfer `.auth/compass` Directory:** Once the `.auth/compass` directory is successfully created locally and contains an authenticated session, you need to transfer this entire directory to your headless server.
    *   Place it in the same relative path within your project structure on the server. For example, if your project is deployed to `/srv/app/my_project/` on the server, the authentication directory should be `/srv/app/my_project/.auth/compass`.
    *   Use secure transfer methods like `scp` or `sftp`. Example using `scp` (run from your local machine):
        ```bash
        # Ensure the .auth directory exists on the server
        ssh your_user@your_server "mkdir -p /srv/app/my_project/.auth"
        # Securely copy the compass session directory
        scp -r .auth/compass your_user@your_server:/srv/app/my_project/.auth/
        ```
    *   **Security Warning:** The `.auth/compass` directory contains sensitive session information. Ensure it's transferred securely and that file permissions on the server are set appropriately to protect it (e.g., restrict read/write access to only the user running the scripts). **Never commit this directory to Git.**

3.  **Run Scripts Headlessly on Server:** With the `.auth/compass` directory in place on the server, your enrichment scripts can now be run using the `--headless` flag. They will automatically use the pre-authenticated session data from the transferred directory.
    ```bash
    python scripts/enrich_compass_to_json.py --headless
    # or
    python scripts/enrich_with_compass.py --headless --limit 10
    # or
    python scripts/enrich_with_compass_details.py --headless --max-listings 5
    ```

4.  **Re-Authentication:** If the session expires over time (Compass.com may invalidate it), you'll need to repeat this process:
    *   Delete the old `.auth/compass` directory from your server.
    *   Re-generate the `.auth/compass` directory locally by running an enrichment script in headed mode and logging in again.
    *   Securely transfer the new `.auth/compass` directory to the server.

### Authentication Notes

*   **Session Validity:** The saved authentication session should remain valid for a considerable period, but Compass.com might invalidate it after some time (e.g., weeks or months). If you encounter authentication errors in headless mode, the first step is usually to regenerate the session locally and re-transfer it as described above.
*   **Security:** The `.auth/compass` directory contains sensitive session information. Ensure this directory is included in your `.gitignore` file (it should be by default if you're using a standard Python gitignore) and is not committed to your repository. Protect it carefully during transfer and on the server.
*   **Multiple Environments:** The note about multiple environments in the "First-Time Authentication Setup" is especially relevant for server setups. Each server or distinct environment where the scripts run will need its own copy of the `.auth/compass` directory, generated and transferred as described.
