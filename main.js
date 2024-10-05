const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');
const util = require('util');
const mime = require('mime-types');
const express = require('express');
const cluster = require('cluster');
const os = require('os');
const { Worker } = require('worker_threads');
const { pipeline } = require('stream');
const bodyParser = require('body-parser');
const http = require('http');
const https = require('https');
const Redis = require('ioredis');
const NodeCache = require('node-cache');
const { promisify } = require('util');

const app = express();
app.use(bodyParser.raw({ type: '*/*', limit: '10gb' }));

const redisClient = new Redis();
const localCache = new NodeCache({ stdTTL: 600, checkperiod: 120 });
const memoryCache = new Map();

const CHUNK_SIZE = 64 * 1024;
const LARGE_FILE_THRESHOLD = 512 * 1024;
const MAX_WORKERS = os.cpus().length * 4;
const CACHE_REFRESH_INTERVAL = 300000; // 5 minutes

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

const loadFile = async (filePath) => {
  const fileData = await fs.readFile(filePath);
  const fileHash = calculateFileHash(fileData);
  const { gzipped, brotli } = await compressFile(fileData);
  const fileInfo = {
    raw: fileData,
    gzipped,
    brotli,
    hash: fileHash,
    mimeType: mime.lookup(filePath) || 'application/octet-stream'
  };

  memoryCache.set(path.basename(filePath), fileInfo);
  localCache.set(path.basename(filePath), fileInfo);
  await redisClient.set(`file:${path.basename(filePath)}`, JSON.stringify({
    raw: fileData.toString('base64'),
    gzipped: gzipped.toString('base64'),
    brotli: brotli.toString('base64'),
    hash: fileHash,
    mimeType: fileInfo.mimeType
  }));

  return fileInfo;
};

const loadAllFiles = async () => {
  const directories = ['cache/game', 'cache/social', 'cache/interface/large'];
  const allFiles = await Promise.all(directories.map(dir =>
    fs.readdir(dir).then(files => files.map(file => path.join(dir, file)))
  ));
  const flattenedFiles = allFiles.flat();
  
  const workerPool = new Array(MAX_WORKERS).fill().map(() => new Worker('./fileWorker.js'));
  let workerIndex = 0;

  await Promise.all(flattenedFiles.map(filePath => {
    return new Promise((resolve, reject) => {
      const worker = workerPool[workerIndex];
      workerIndex = (workerIndex + 1) % MAX_WORKERS;
      worker.postMessage({ filePath });
      worker.once('message', resolve);
      worker.once('error', reject);
    });
  }));

  workerPool.forEach(worker => worker.terminate());
};

const refreshCache = async () => {
  const keys = await redisClient.keys('file:*');
  for (const key of keys) {
    const fileName = key.split(':')[1];
    const cachedFile = await redisClient.get(key);
    if (cachedFile) {
      const fileInfo = JSON.parse(cachedFile);
      memoryCache.set(fileName, {
        raw: Buffer.from(fileInfo.raw, 'base64'),
        gzipped: Buffer.from(fileInfo.gzipped, 'base64'),
        brotli: Buffer.from(fileInfo.brotli, 'base64'),
        hash: fileInfo.hash,
        mimeType: fileInfo.mimeType
      });
      localCache.set(fileName, fileInfo);
    }
  }
};

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);
  loadAllFiles().then(() => {
    for (let i = 0; i < os.cpus().length * 2; i++) {
      cluster.fork();
    }
    setInterval(refreshCache, CACHE_REFRESH_INTERVAL);
  });
  cluster.on('exit', (worker) => {
    console.log(`Worker ${worker.process.pid} died`);
    cluster.fork();
  });
} else {
  (async () => {
    const [keyperm, certperm] = await Promise.all([
      fs.readFile('./key.pem'),
      fs.readFile('./cert.pem')
    ]);

    const httpsServer = https.createServer({ key: keyperm, cert: certperm }, app);

    const port = 443;
    const ipvps = process.env.VPS_IP || '127.0.0.1';
    const udpport = process.env.UDP_PORT || '17091';

    const php = `server|${ipvps}\nport|${udpport}\ntype|1\nbeta_server|127.0.0.1\nbeta_port|17091\nbeta_type|1\nmeta|defined\nRTENDMARKERBS1001|unknown`;

    app.get('/cache/*', async (req, res) => {
      const fileName = path.basename(req.params[0]);
      const filePath = path.join(__dirname, 'cache', req.params[0]);

      try {
        let fileInfo;

        if (memoryCache.has(fileName)) {
          fileInfo = memoryCache.get(fileName);
        } else if (localCache.has(fileName)) {
          fileInfo = localCache.get(fileName);
          memoryCache.set(fileName, fileInfo);
        } else {
          const cachedFile = await redisClient.get(`file:${fileName}`);
          if (cachedFile) {
            fileInfo = JSON.parse(cachedFile);
            fileInfo.raw = Buffer.from(fileInfo.raw, 'base64');
            fileInfo.gzipped = Buffer.from(fileInfo.gzipped, 'base64');
            fileInfo.brotli = Buffer.from(fileInfo.brotli, 'base64');
            memoryCache.set(fileName, fileInfo);
            localCache.set(fileName, fileInfo);
          } else {
            fileInfo = await loadFile(filePath);
          }
        }

        res.set({
          'Content-Type': fileInfo.mimeType,
          'Content-Disposition': `attachment; filename=${fileName}`,
          'ETag': fileInfo.hash,
          'Cache-Control': 'public, max-age=31536000'
        });

        if (req.headers['accept-encoding']?.includes('br')) {
          res.set('Content-Encoding', 'br');
          res.end(fileInfo.brotli);
        } else if (req.headers['accept-encoding']?.includes('gzip')) {
          res.set('Content-Encoding', 'gzip');
          res.end(fileInfo.gzipped);
        } else {
          res.end(fileInfo.raw);
        }
      } catch (error) {
        console.error(`Error serving file ${fileName}:`, error);
        res.status(404).send('File not found or corrupted.');
      }
    });

    app.post('/growtopia/server_data.php', (req, res) => res.status(200).send(php));
    app.use((req, res) => res.status(200).send(php));

    httpsServer.listen(port, () => console.log(`Worker ${process.pid} - HTTPS server running on port ${port}`));

    http.createServer((req, res) => {
      if (req.url === '/' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('HTTPS - GTPS');
      } else if (req.url === '/growtopia/server_data.php' && req.method === 'POST') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end(php);
      } else {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('404 Not Found');
      }
    }).listen(80);
  })();
}
