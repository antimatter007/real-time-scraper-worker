// worker/index.js

const amqp = require('amqplib');
const { Pool } = require('pg');
const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

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
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled',
      '--disable-gpu',
      '--disable-dev-shm-usage',
      '--disable-extensions',
      '--disable-infobars',
      '--window-size=1920,1080',
    ],
  });

  const page = await browser.newPage();

  // Set a realistic user-agent and accept language headers
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
    'AppleWebKit/537.36 (KHTML, like Gecko) ' +
    'Chrome/115.0.0.0 Safari/537.36');
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9'
  });

  page.setDefaultTimeout(60000);
  page.setDefaultNavigationTimeout(60000);

  const url = `https://www.reddit.com/search?q=${encodeURIComponent(query)}&sort=relevance&t=all`;
  console.log(`Navigating to ${url}...`);
  
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    
    // Optional: Wait a bit to let Reddit load dynamic content
    await page.waitForTimeout(5000);
    
    // Debugging screenshot
    const screenshotPath = path.join(__dirname, `screenshot_${Date.now()}.png`);
    await page.screenshot({ path: screenshotPath, fullPage: true });
    console.log(`Screenshot saved to ${screenshotPath}`);

    const pageTitle = await page.title();
    console.log(`Page Title: "${pageTitle}"`);
    if (!pageTitle || pageTitle.trim() === '') {
      console.warn('Page title is empty. Possible redirection or block.');
    }

    // Check page content for captcha or block messages
    const htmlContent = await page.content();
    if (htmlContent.includes('are you a human') || htmlContent.includes('captcha')) {
      console.warn('It looks like we hit a CAPTCHA or human verification page.');
      // Handle CAPTCHA here if possible or abort.
    }

    // Attempt to find the search post units
    await page.waitForSelector('div[data-testid="search-post-unit"]', { timeout: 60000 });
    console.log('Post containers found.');

    // Scroll to load more if needed
    await page.evaluate(() => { window.scrollBy(0, window.innerHeight); });
    await page.waitForTimeout(3000);

    console.log('Extracting posts...');
    const posts = await page.evaluate(() => {
      const postContainers = document.querySelectorAll('div[data-testid="search-post-unit"]');
      const data = [];
      let count = 0;

      for (let container of postContainers) {
        if (count >= 10) break; // Limit to 10 posts

        const titleElement = container.querySelector('a[data-testid="post-title-text"]');
        if (!titleElement) continue;

        const tweet_text = titleElement.innerText.trim() || 'No Title';
        const tweet_href = titleElement.getAttribute('href') || '';
        const postIdMatch = tweet_href.match(/comments\/([^/]+)\//);
        const tweet_id = postIdMatch ? `t3_${postIdMatch[1]}` : `post_${count}`;

        const authorLink = container.querySelector('a[href*="/user/"]');
        const author_handle = authorLink ? authorLink.innerText.trim() : 'unknown';

        const timeElement = container.querySelector('time');
        let timestamp = new Date().toISOString();
        if (timeElement && timeElement.getAttribute('datetime')) {
          timestamp = new Date(timeElement.getAttribute('datetime')).toISOString();
        }

        data.push({ tweet_id, tweet_text, author_handle, timestamp });
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
