# Property Pipeline Explorer

A Streamlit application to navigate, filter, and explore property listings extracted from Gmail.

## Setup Instructions

1. **Install Required Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Database Path**

   Place your SQLite database file (or update the path in the app) in the same directory as the app.

3. **Run the Streamlit App**

   ```bash
   streamlit run property_pipeline_app.py
   ```

4. **Access the App**

   The app will be available at:
   - Local URL: http://localhost:8501
   - Network URL: http://your-ip-address:8501

## Features

- **Table Selection**: View different tables from your property pipeline database
- **Column Selection**: Choose which columns to display
- **Dynamic Filtering**: Filter records based on property values
- **Sorting**: Order properties by various criteria
- **URL Handling**: Clickable links to property listings
- **Data Export**: Download filtered data in CSV or JSON format
- **Basic Statistics**: View numeric summaries of your data

## Customization

You can customize this app by:

1. Adding more advanced filters
2. Creating data visualizations for property metrics
3. Implementing saved searches or favorites
4. Adding user authentication if needed
5. Creating additional views or pages for different types of analysis

## Deployment

To deploy on a Linux/Unix server:

1. Set up a virtual environment
2. Install dependencies
3. Use one of these methods:
   - Direct Streamlit serving
   - Docker containerization
   - Nginx/Apache with reverse proxy

For production environments, consider setting up proper authentication and database security.
