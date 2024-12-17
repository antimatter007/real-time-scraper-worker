const amqp = require('amqplib');
const { Pool } = require('pg');
const puppeteer = require('puppeteer');

// Define queue name
const queueName = 'jobs_queue';

// We no longer need Twitter credentials since we are scraping Reddit.
const TWITTER_USERNAME = 'patrickbatman16';
const TWITTER_PASSWORD = 'Ankitsp@007';
const TWITTER_EMAIL = 'ankitp.ecell@gmail.com'; 
// These are now unused, but we'll leave them as is to not change code structure.
// They won't affect Reddit scraping.

// Hardcoded PostgreSQL Configuration (unchanged)
const pool = new Pool({
  host: 'autorack.proxy.rlwy.net',
  port: 20823,
  database: 'railway',
  user: 'postgres',
  password: 'suFzdtdvTXFdhgQloNbxzOHMjLsisThP',
  ssl: {
    rejectUnauthorized: false, 
  },
});

pool.connect()
  .then(() => console.log('Worker connected to PostgreSQL'))
  .catch(err => console.error('Worker connection error:', err.stack));

// Hardcoded RabbitMQ Configuration (unchanged)
const RABBITMQ_URL = 'amqps://pcudcyxc:CT6kMcrw_pXH7kFpqzpqWgoWnu5J04LU@duck.lmq.cloudamqp.com/pcudcyxc';

let channel;

// Connect to RabbitMQ (unchanged)
async function connectRabbitMQ() {
  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    channel = await conn.createChannel();
    await channel.assertQueue(queueName, { durable: true });
    console.log('Connected to RabbitMQ');
  } catch (error) {
    console.error('Failed to connect to RabbitMQ:', error);
    process.exit(1);
  }
}

// Utility Function for Delay (unchanged)
async function delay(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

// Rename and rewrite the scraping function to scrape Reddit instead of Twitter.
// query will be a subreddit name, e.g., "programming" -> https://www.reddit.com/r/programming/
async function scrapeReddit(query) {
  console.log(`Starting scrape for subreddit: "${query}"`);

  const browser = await puppeteer.launch({
    headless: true, // enable headless
    slowMo: 50,
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
    const url = `https://www.reddit.com/r/${query}/`;
    console.log(`Navigating to ${url}...`);
    await page.goto(url, { waitUntil: 'networkidle2' });

    // Wait for posts to render
    await delay(5000);

    // Extract top 10 posts
    console.log('Extracting posts...');
    const posts = await page.evaluate(() => {
      const postElements = document.querySelectorAll('div[data-testid="post-container"]');
      const data = [];
      let count = 0;
      for (let post of postElements) {
        if (count >= 10) break;

        // Extract a unique ID for the post
        // Reddit often uses data-fullname attribute for posts, like t3_xxxxxxx
        const postId = post.getAttribute('data-fullname') || `post_${count}`;

        // Extract title (this will serve as tweet_text equivalent)
        const titleEl = post.querySelector('h3');
        const postText = titleEl ? titleEl.innerText : 'No Title';

        // Extract author handle (username)
        const authorLink = post.querySelector('a[href*="/user/"]');
        const authorHandle = authorLink ? authorLink.innerText : 'unknown';

        // Use current time as timestamp (since exact post time might require parsing)
        const timestamp = new Date().toISOString();

        data.push({
          tweet_id: postId,
          tweet_text: postText,
          author_handle: authorHandle,
          timestamp: timestamp,
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

// Consume Jobs from RabbitMQ (unchanged logic, just call scrapeReddit now)
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
          // Now we use scrapeReddit instead of scrapeTwitter
          const posts = await scrapeReddit(query);

          // Insert results into DB (same fields, just different data)
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
