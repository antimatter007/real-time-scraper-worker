// worker/index.js

const amqp = require('amqplib');
const { Pool } = require('pg');
const puppeteer = require('puppeteer');

// Define queue name
const queueName = 'jobs_queue';

// Hardcoded PostgreSQL Configuration
const pool = new Pool({
  host: 'autorack.proxy.rlwy.net',
  port: 20823,
  database: 'railway',
  user: 'postgres',
  password: 'suFzdtdvTXFdhgQloNbxzOHMjLsisThP',
  ssl: {
    rejectUnauthorized: false, // Set to true if you have proper SSL certificates
  },
});

// Connect to PostgreSQL
pool.connect()
  .then(() => console.log('Worker connected to PostgreSQL'))
  .catch(err => console.error('Worker connection error:', err.stack));

// Hardcoded RabbitMQ Configuration
const RABBITMQ_URL = 'amqps://pcudcyxc:CT6kMcrw_pXH7kFpqzpqWgoWnu5J04LU@duck.lmq.cloudamqp.com/pcudcyxc';

let channel;

// Function to Connect to RabbitMQ
async function connectRabbitMQ() {
  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    channel = await conn.createChannel();
    await channel.assertQueue(queueName, { durable: true });
    console.log('Connected to RabbitMQ');
  } catch (error) {
    console.error('Failed to connect to RabbitMQ:', error);
    process.exit(1); // Exit if connection fails
  }
}

// Utility Function for Delay
async function delay(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

// Function to Scrape Reddit Using Search URL
async function scrapeReddit(query) {
  console.log(`Starting scrape for query: "${query}"`);

  const browser = await puppeteer.launch({
    headless: false, // Ensure headless mode is enabled
    defaultViewport: null,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-gpu',
      '--disable-dev-shm-usage',
    ],
  });

  const page = await browser.newPage();

  // Increase timeouts
  page.setDefaultTimeout(60000);
  page.setDefaultNavigationTimeout(60000);

  // Set a user agent
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
    'AppleWebKit/537.36 (KHTML, like Gecko) ' +
    'Chrome/115.0.0.0 Safari/537.36'
  );

  try {
    // Navigate to Reddit's search page
    const url = `https://www.reddit.com/search?q=${encodeURIComponent(query)}&sort=relevance&t=all`;
    console.log(`Navigating to ${url}...`);
    await page.goto(url, { waitUntil: 'networkidle2' });

    // Wait for posts to load by waiting for the post title selector
    await page.waitForSelector('a[data-testid="post-title"]', { timeout: 15000 });

    // Optionally scroll to load more posts
    await page.evaluate(() => { window.scrollBy(0, window.innerHeight); });
    await delay(3000); // Wait for additional posts to load

    console.log('Extracting posts...');
    const posts = await page.evaluate(() => {
      const postTitleElements = document.querySelectorAll('a[data-testid="post-title"]');
      const data = [];
      let count = 0;
      for (let el of postTitleElements) {
        if (count >= 10) break;

        const tweet_id = el.getAttribute('href') || `post_${count}`;
        const tweet_text = el.innerText || 'No Title';

        // Traverse up the DOM to find the author
        let author_handle = 'unknown';
        let parent = el.parentElement;
        while (parent) {
          const authorLink = parent.querySelector('a[href*="/user/"]');
          if (authorLink) {
            author_handle = authorLink.innerText;
            break;
          }
          parent = parent.parentElement;
        }

        // Extract timestamp
        let timestamp = new Date().toISOString(); // Default to current time
        const timeElement = parent.querySelector('a[data-click-id="timestamp"] > time');
        if (timeElement) {
          const datetime = timeElement.getAttribute('datetime');
          if (datetime) {
            timestamp = new Date(datetime * 1000).toISOString();
          }
        }

        data.push({
          tweet_id,
          tweet_text,
          author_handle,
          timestamp,
        });
        count++;
      }
      return data;
    });

    console.log(`Number of posts extracted: ${posts.length}`);
    await browser.close();
    return posts;
  } catch (error) {
    console.error('Error during scraping:', error);
    await browser.close();
    throw error;
  }
}

// Function to Consume Jobs from RabbitMQ
async function consumeJobs() {
  try {
    if (!channel) {
      throw new Error('Channel is not defined');
    }
    channel.consume(
      queueName,
      async (msg) => {
        if (!msg) return;
        const { jobId, query } = JSON.parse(msg.content.toString());
        console.log(`Received job ${jobId} with query "${query}"`);

        // Mark job as in_progress
        await pool.query('UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2', [
          'in_progress',
          jobId,
        ]);

        try {
          // Scrape Reddit using the updated function
          const posts = await scrapeReddit(query);

          if (posts.length === 0) {
            console.warn(`No posts found for job ${jobId} with query "${query}"`);
          }

          // Insert results into DB
          for (const p of posts) {
            await pool.query(
              'INSERT INTO results (job_id, tweet_id, tweet_text, author_handle, timestamp) VALUES ($1, $2, $3, $4, $5)',
              [jobId, p.tweet_id, p.tweet_text, p.author_handle, p.timestamp]
            );
          }

          // Update job status to completed
          await pool.query('UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2', [
            'completed',
            jobId,
          ]);
          console.log(`Job ${jobId} completed successfully with ${posts.length} posts.`);
        } catch (err) {
          console.error(`Scrape failed for job ${jobId}:`, err);
          await pool.query('UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2', [
            'failed',
            jobId,
          ]);
          console.log(`Job ${jobId} marked as failed.`);
        }

        // Acknowledge the message
        channel.ack(msg);
      },
      {
        noAck: false,
      }
    );

    console.log('Worker is waiting for messages...');
  } catch (error) {
    console.error('Error consuming jobs:', error);
  }
}

// Initialize the connection and start consuming
async function init() {
  await connectRabbitMQ();
  await consumeJobs();
}

init().catch(console.error);
