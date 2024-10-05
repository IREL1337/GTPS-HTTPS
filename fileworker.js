const fs = require('fs').promises;
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');
const { parentPort } = require('worker_threads');
const Redis = require('ioredis');
const { promisify } = require('util');

const redisClient = new Redis();

const gzipAsync = promisify(zlib.gzip);
const brotliCompressAsync = promisify(zlib.brotliCompress);

const calculateFileHash = (fileData) => {
  return crypto.createHash('xxhash64').update(fileData).digest('hex');
};

const compressFile = async (fileData) => {
  const [gzipped, brotli] = await Promise.all([
    gzipAsync(fileData, { level: zlib.constants.Z_BEST_SPEED }),
    brotliCompressAsync(fileData, {
      params: {
        [zlib.constants.BROTLI_PARAM_MODE]: zlib.constants.BROTLI_MODE_GENERIC,
        [zlib.constants.BROTLI_PARAM_QUALITY]: 4,
        [zlib.constants.BROTLI_PARAM_SIZE_HINT]: fileData.length
      }
    })
  ]);
  return { gzipped, brotli };
};

parentPort.on('message', async ({ filePath }) => {
  try {
    const fileData = await fs.readFile(filePath);
    const fileHash = calculateFileHash(fileData);
    const { gzipped, brotli } = await compressFile(fileData);
    
    await redisClient.set(`file:${path.basename(filePath)}`, JSON.stringify({
      raw: fileData.toString('base64'),
      gzipped: gzipped.toString('base64'),
      brotli: brotli.toString('base64'),
      hash: fileHash,
      mimeType: mime.lookup(filePath) || 'application/octet-stream'
    }));
    
    parentPort.postMessage({ success: true, filePath });
  } catch (error) {
    parentPort.postMessage({ success: false, filePath, error: error.message });
  }
});
