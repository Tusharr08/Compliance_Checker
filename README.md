# Compliance Checker

**Compliance Checker** is a command-line application designed to help organizations ensure their GitHub repositories adhere to compliance standards. This tool connects to the organization's account, accesses all repositories, and checks `.yml` or `.yaml` workflow files for compliance.

## **Table of Contents**
1. [Introduction](#introduction)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Contributing](#contributing)
6. [License](#license)

## **Introduction**

*Compliance Checker* aims to provide a flexible solution for developers who need to ensure their GitHub workflows comply with organizational standards.

## **Features**
- Connects seamlessly with your organization’s GitHub account.
- Accesses all repositories within the organization.
- Identifies and analyzes `.yml` or `.yaml` workflow files.
- Checks each file against predefined compliance rules.
- Generates detailed reports on compliance status.

## **Installation**
To install Compliance Checker, follow these steps:

1. Clone the repository:
    ```bash
    git clone https://github.com/503425502/compliance-checker.git
    ```
2. Navigate into the project directory:
    ```bash
    cd compliance-checker
    ```
3. Install dependencies:
    ```bash
    python install -r requirements.txt 
    ```

4. Setup Environment variables:
    ```bash
    source .env
    ```

## **Usage**
To use Compliance Checker:

1. Activate the Virtual Environment
    ```bash
    venv\Scripts\activate
    ```

2. Run the application using Python:
   ```bash
   python main.py
   ```
3. Deactive the Virtual environment when done:
    ```bash
    deactivate
    ```