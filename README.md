# Daily Data Fetch and Update

This project consists of a Python script that fetches product data from an external API, processes it, and saves the resulting data as a CSV file in the repository. A GitHub Action is set up to run this script daily at 12:00 PM, automatically updating the CSV file with the latest data.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [GitHub Actions Workflow](#github-actions-workflow)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [License](#license)

## Overview

The main purpose of this project is to automate the process of data collection and storage. By running the Python script daily, we ensure that the data is always up-to-date. The script performs the following tasks:

- Sends an HTTP GET request to a specified API endpoint.
- Parses the JSON response to extract product, merchant, supplier, and category information.
- Processes and organizes the data into a pandas DataFrame.
- Saves the DataFrame as a CSV file (`data.csv`) in the repository.

## Features

- **Automated Data Collection**: Fetches and updates data daily without manual intervention.
- **Data Processing**: Cleans and structures raw JSON data into a readable CSV format.
- **Continuous Integration**: Utilizes GitHub Actions for scheduled execution and data updates.
- **Customizable**: Easily modify the script to fetch different data or adjust processing logic.

## Setup

### Prerequisites

- Python 3.x installed on your local machine.
- Git installed for version control.
- A GitHub account and repository where you have write access.

### Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/troboukis/ekatanalotis.git
   cd your-repo-name
   ```

2. **Install Required Python Packages**

   It's recommended to use a virtual environment.

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   pip install -r requirements.txt
   ```

3. **Verify the Script**

   Ensure the script runs without errors.

   ```bash
   python script.py
   ```

   This should generate a `data.csv` file in your repository.

## GitHub Actions Workflow

The project includes a GitHub Actions workflow defined in `.github/workflows/daily_run.yml`. This workflow automates the execution of the `script.py` every day at 12:00 PM UTC and commits any changes to `data.csv` back to the repository.

### Workflow Breakdown

- **Schedule Trigger**

  ```yaml
  on:
    schedule:
      - cron: '0 12 * * *'
  ```

  The workflow is scheduled to run daily at 12:00 PM UTC.

- **Jobs and Steps**

  The workflow consists of the following main steps:

  1. **Checkout Repository**

     ```yaml
     - name: Checkout repository
       uses: actions/checkout@v3
       with:
         persist-credentials: false
     ```

     Checks out the repository code.

  2. **Set Up Python Environment**

     ```yaml
     - name: Set up Python
       uses: actions/setup-python@v4
       with:
         python-version: '3.x'
     ```

     Sets up Python 3.x on the runner.

  3. **Install Dependencies**

     ```yaml
     - name: Install dependencies
       run: |
         python -m pip install --upgrade pip
         pip install requests pandas numpy
     ```

     Installs necessary Python packages.

  4. **Run Python Script**

     ```yaml
     - name: Run Python script
       run: python script.py
     ```

     Executes the `script.py` file.

  5. **Commit and Push Changes**

     ```yaml
     - name: Commit changes
       run: |
         git config --global user.name 'github-actions[bot]'
         git config --global user.email 'github-actions[bot]@users.noreply.github.com'
         git add data.csv
         git commit -m 'Update data.csv'
         git push
       env:
         GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
     ```

     Commits the updated `data.csv` and pushes it back to the repository.

## Usage

- **Manual Execution**

  You can manually run the script using:

  ```bash
  python script.py
  ```

- **Viewing Data**

  The `data.csv` file will contain the latest data after each run. You can open it with any CSV viewer or import it into data analysis tools.

- **Monitoring Workflow**

  - Navigate to the **Actions** tab in your GitHub repository to monitor workflow runs.
  - You can see logs, run history, and manually trigger the workflow if needed.

## Project Structure

```
your-repo-name/
├── .github/
│   └── workflows/
│       └── daily_run.yml   # GitHub Actions workflow file
├── script.py               # Main Python script
├── data.csv                # Generated CSV data file
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation
└── LICENSE                 # License information
```

## Dependencies

- **Python Packages**

  - `requests`: For making HTTP requests.
  - `pandas`: For data manipulation and analysis.
  - `numpy`: For numerical operations.

- **GitHub Actions**

  - Uses `ubuntu-latest` runner.
  - Requires no additional setup beyond what's specified in the workflow file.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes or additions you'd like to make.

**Steps to Contribute:**

1. Fork the repository.
2. Create a new branch: `git checkout -b feature/YourFeature`.
3. Commit your changes: `git commit -m 'Add your message'`.
4. Push to the branch: `git push origin feature/YourFeature`.
5. Open a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

---

**Note:** Adjust the repository URLs, usernames, and any placeholders (`your-username`, `your-repo-name`) to match your actual GitHub repository details.
