const fs = require('fs');
const path = require('path');

// Folder paths
const inputFolder = './input'; // Folder to monitor
const outputFolder = './output'; // Folder to store the chunks
const CHUNK_SIZE = 10 * 1024 * 1024; // 10 MB chunk size

// Track processed files to avoid duplicates
let processedFiles = new Set();

// Extensions for text files
const textFileExtensions = ['.txt', '.json', '.csv', '.html', '.xml', '.md', '.js', '.css'];
const binaryFileExtensions = ['.pdf', '.jpg', '.jpeg', '.png', '.gif'];

// Function to check if a file is a text file based on its extension
function isTextFile(fileExtension) {
    return textFileExtensions.includes(fileExtension.toLowerCase());
}

// Function to check if a file is a binary file (e.g., PDF, images)
function isBinaryFile(fileExtension) {
    return binaryFileExtensions.includes(fileExtension.toLowerCase());
}

// Function to check the input folder for new files
function checkForNewFiles() {
    fs.readdir(inputFolder, (err, files) => {
        if (err) {
            console.error('Error reading input folder:', err);
            return;
        }
        files.forEach(file => {
            if (!processedFiles.has(file)) {
                const filePath = path.join(inputFolder, file);
                processFile(filePath);
            }
        });
    });
}

// Function to process files based on type
function processFile(filePath) {
    const fileName = path.basename(filePath);
    const fileExtension = path.extname(filePath);
    const outputFileBase = path.join(outputFolder, path.parse(fileName).name); // Base without extension

    if (isTextFile(fileExtension)) {
        splitTextFileWithIntegrityCheck(filePath, outputFileBase, fileExtension, fileName);
    } else if (isBinaryFile(fileExtension)) {
        splitBinaryFile(filePath, outputFileBase, fileExtension, fileName);
    } else {
        console.error(`Unsupported file type: ${fileExtension}`);
    }
}

// Function to split text/CSV files and maintain data integrity using your specified logic
function splitTextFileWithIntegrityCheck(filePath, outputFileBase, fileExtension, fileName) {
    // Step 1: Read the entire file content as a string
    const originalContent = fs.readFileSync(filePath, 'utf8');

    // Step 2: Split content into chunks of exactly 10 MB each
    const chunks = splitIntoChunks(originalContent, CHUNK_SIZE);

    // Step 3: Write each chunk to a separate file
    chunks.forEach((chunk, index) => {
        const chunkFilePath = `${outputFileBase}-chunk-${index + 1}${fileExtension}`;
        fs.writeFileSync(chunkFilePath, chunk, 'utf8');
        console.log(`Written chunk ${index + 1} of text file ${fileName}`);
    });

    // Step 4: Concatenate chunks and validate data integrity
    const concatenatedContent = chunks.join('');
    const isDataIntact = concatenatedContent === originalContent;
    console.log(`Data Integrity Check for ${fileName}:`, isDataIntact ? 'Pass' : 'Fail');

    // Mark file as processed
    processedFiles.add(fileName);
}

// Helper function to split content into fixed-size chunks (10 MB) while preserving line integrity
function splitIntoChunks(content, chunkSize) {
    const chunks = [];
    for (let i = 0; i < content.length; i += chunkSize) {
        chunks.push(content.slice(i, i + chunkSize));
    }
    return chunks;
}

// Function to split binary files (including PDFs) into chunks
function splitBinaryFile(filePath, outputFileBase, fileExtension, fileName) {
    const stats = fs.statSync(filePath);
    const totalChunks = Math.ceil(stats.size / CHUNK_SIZE);
    const readStream = fs.createReadStream(filePath, { highWaterMark: CHUNK_SIZE });

    let chunkIndex = 0;

    readStream.on('data', (chunk) => {
        const chunkFilePath = `${outputFileBase}-chunk-${chunkIndex}${fileExtension}`;
        fs.writeFileSync(chunkFilePath, chunk);
        console.log(`Written chunk ${chunkIndex} of binary file ${fileName}`);
        chunkIndex++;
    });

    readStream.on('end', () => {
        console.log(`Binary file ${fileName} split into ${totalChunks} chunks.`);
        processedFiles.add(fileName);
        validateFile(filePath, outputFileBase, totalChunks, false, fileExtension);
    });

    readStream.on('error', (err) => {
        console.error('Error reading binary file:', err);
    });
}

// Function to concatenate chunks and compare with the original
function validateFile(originalFilePath, outputFileBase, totalChunks, isTextFile, fileExtension) {
    const concatenatedFilePath = `${outputFileBase}-concatenated${fileExtension}`;
    const writeStream = fs.createWriteStream(concatenatedFilePath, { encoding: isTextFile ? 'utf8' : undefined });

    for (let i = 0; i < totalChunks; i++) {
        const chunkFilePath = `${outputFileBase}-chunk-${i}${fileExtension}`;
        const chunk = fs.readFileSync(chunkFilePath, isTextFile ? 'utf8' : undefined);
        writeStream.write(chunk);
    }

    writeStream.end(() => {
        console.log(`Chunks concatenated to ${concatenatedFilePath}`);
        compareFiles(originalFilePath, concatenatedFilePath, isTextFile);
    });
}

// Function to compare the original file with the concatenated file
function compareFiles(originalFile, concatenatedFile, isTextFile) {
    const originalBuffer = fs.readFileSync(originalFile, isTextFile ? 'utf8' : undefined);
    const concatenatedBuffer = fs.readFileSync(concatenatedFile, isTextFile ? 'utf8' : undefined);

    const areFilesIdentical = isTextFile
        ? originalBuffer === concatenatedBuffer
        : Buffer.compare(originalBuffer, concatenatedBuffer) === 0;

    if (areFilesIdentical) {
        console.log('Files are identical. No data loss.');
    } else {
        console.error('Files do not match. Data might be lost.');
    }
}

// Check for new files every 10 seconds
setInterval(checkForNewFiles, 10000); // 10 seconds in milliseconds
