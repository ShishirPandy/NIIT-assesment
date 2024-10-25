# Node.js Real-Time File Handling Application

## Overview

This Node.js application is designed to handle large files in real-time. It monitors an `input` folder for new files, automatically splits them into smaller, manageable 10MB chunks, concatenates those chunks back into the original file, and verifies file integrity using MD5 hashes. Email notifications are sent upon successful processing.

## Features

- **Real-time File Monitoring**: Monitors the `input` folder for new files using `chokidar`.
- **File Splitting**: Splits files into 10MB chunks and stores them in the `output` folder.
- **File Concatenation**: Recombines the chunks back into a single file.
- **File Integrity Check**: Verifies file integrity using MD5 hashing to ensure no data is lost.
- **Email Notifications**: Sends email notifications upon successful file processing.

## Requirements

- **Node.js**: Ensure you have Node.js installed. Download it from [here](https://nodejs.org/).
- **Gmail Account**: A Gmail account is required for sending email notifications. If you're using 2FA (two-factor authentication), generate an app-specific password.

## Installation

1. Clone or download the repository.

   ```bash
   git clone https://github.com/ShishirPandy/file-handling-app.git
   cd  file-handling-app

2. ```bash
   npm install

3. ```bash 
   mkdir input output

## Configure your Gmail credentials for email notifications:

- **Open the index.js file**.
- **Replace the placeholder email and password with your Gmail credentials or app-specific password if using 2FA**.

## Usage
To run the application, use:
   ```bash 
   node index.js
```
![image](https://github.com/user-attachments/assets/2754f6b4-3013-47cb-a367-54e7e9b7c141)

Once the application is running, it monitors the input folder for new files. When a new file is added, the following process occurs:

-**File Splitting:** The file is split into 10MB chunks and saved in the output folder.
-**Concatenation:** All chunks are concatenated back into a single file.
-**File Integrity Verification:** MD5 hashes of the original and concatenated files are compared to ensure no data loss

## Example
Place a file (e.g., largefile.txt) in the input folder.
The application will split the file, verify integrity, and send an email once done.
You can find the processed chunks and the concatenated file in the output folder.

## Gmail Setup

If you're using Gmail with two-factor authentication (2FA), you'll need to generate an app-specific password:
Visit your Google Account.
Go to Security > App Passwords under the "Signing in to Google" section.
Generate an app-specific password and use it in place of your Gmail password in the nodemailer configuration.

## License

This project is licensed under the MIT License.



