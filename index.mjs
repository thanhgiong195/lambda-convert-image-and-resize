import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import sharp from 'sharp';
import heicConvert from 'heic-convert';
import path from 'path';

const s3 = new S3Client({ region: 'ap-northeast-1' });

// Helper: collect various Body stream types into a Buffer (works in Lambda Node.js)
async function bodyToBuffer(Body) {
    // AWS SDK v3 in Node returns a Node.js Readable stream
    if (Body && typeof Body.read === 'function') {
        const chunks = [];
        for await (const chunk of Body) {
            chunks.push(chunk);
        }
        return Buffer.concat(chunks);
    }
    // Web streams (unlikely in Lambda, but handle just in case)
    if (Body && typeof Body.getReader === 'function') {
        const reader = Body.getReader();
        const chunks = [];
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(Buffer.from(value));
        }
        return Buffer.concat(chunks);
    }
    // Some environments provide transformToByteArray
    if (Body && typeof Body.transformToByteArray === 'function') {
        const arr = await Body.transformToByteArray();
        return Buffer.from(arr);
    }
    // Fallback
    throw new Error('Unsupported S3 Body type');
}

export const handler = async (event, context) => {
    console.log('Lambda started with event:', JSON.stringify(event, null, 2));
    const srcBucket = event.Records[0].s3.bucket.name;
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    console.log('Processing file:', srcKey);

    // Idempotency: use object metadata to skip already-processed files
    try {
        const head = await s3.send(new HeadObjectCommand({ Bucket: srcBucket, Key: srcKey }));
        const meta = (head.Metadata || {});
        const processed = (meta['image-processed'] || meta['Image-Processed'] || '').toString().toLowerCase() === 'true';
        if (processed) {
            console.log('Object already processed (metadata flag present). Skipping.');
            return;
        }
    } catch (e) {
        // If HeadObject fails (e.g., permissions), log and continue; processing may still succeed.
        console.warn('HeadObject failed or not permitted; proceeding without metadata check:', e?.name || e);
    }

    try {
        // Robustly determine file extension and basename using last dot and POSIX path semantics
        const parsed = path.posix.parse(srcKey);
        const dir = parsed.dir; // may be ''
        const baseNameNoExt = parsed.name; // filename without extension
        const ext = (parsed.ext || '').replace('.', '');
        const imageType = ext.toLowerCase();
        console.log('File type:', imageType);

        // if (!["jpg", "jpeg", "png", "heic"].includes(imageType)) {
        //     console.log(`Unsupported image type: ${imageType}`);
        //     return;
        // }
        if (!["heic"].includes(imageType)) {
            console.log(`Unsupported image type: ${imageType}`);
            return;
        }

        console.log('Fetching object from S3:', srcKey);
        const { Body } = await s3.send(new GetObjectCommand({
            Bucket: srcBucket,
            Key: srcKey
        }));
        console.log('S3 object fetched');

        let contentBuffer = await bodyToBuffer(Body);
        console.log('Buffer size:', contentBuffer.length);

        const width = 1000;
        console.log('Processing image with sharp:', srcKey);

        let workingBuffer = contentBuffer;
        if (imageType === 'heic') {
            console.log('Converting HEIC to JPEG buffer using heic-convert');
            workingBuffer = await heicConvert({
                buffer: contentBuffer,
                format: 'JPEG',
                quality: 1
            });
            console.log('HEIC converted to JPEG buffer, size:', workingBuffer.length);
        }

        let sharpInstance = sharp(workingBuffer);
        if (imageType === 'heic') {
            sharpInstance = sharpInstance.jpeg({ quality: 100 });
        }

        const resizedBuffer = await sharpInstance.resize(width).toBuffer();
        console.log('Image processed, output size:', resizedBuffer.length);

        const outputFiletype = imageType === 'heic' ? 'jpg' : (imageType === 'jpg' ? 'jpeg' : imageType);
        console.log('Overwriting original object with processed image:', srcKey);
        const putResult = await s3.send(new PutObjectCommand({
            Bucket: srcBucket,
            Key: srcKey,
            Body: resizedBuffer,
            ContentType: `image/${outputFiletype}`,
            Metadata: {
                'image-processed': 'true',
                'processed-width': '1000',
                'original-ext': imageType,
                'output-format': outputFiletype
            }
        }));
        console.log('Object overwritten with processed image:', putResult);

        return `Processed and overwritten ${srcBucket}/${srcKey} (format: ${outputFiletype})`;
    } catch (error) {
        console.error('Error processing file:', error);
        throw error;
    }
};