// worker/index.js

const amqp = require('amqplib');
const { Pool } = require('pg');
const puppeteer = require('puppeteer');

// Define queue name
const queueName = 'jobs_queue';

// Hardcoded Twitter Credentials
const TWITTER_USERNAME = 'patrickbatman16';
const TWITTER_PASSWORD = 'Ankitsp@007';
const TWITTER_EMAIL = 'ankitp.ecell@gmail.com'; 

// Ensure credentials are provided
if (!TWITTER_USERNAME || !TWITTER_PASSWORD || !TWITTER_EMAIL) {
  console.error('Twitter credentials (username, password, email) are not set.');
  process.exit(1);
}

// Hardcoded PostgreSQL Configuration
const pool = new Pool({
  host: 'autorack.proxy.rlwy.net',
  database: 'railway',
  user: 'postgres',
  password: 'fzBKMaLxqMFZKWLXEnnAoqSwUAMslaMm',
  port: 29248,
  ssl: {
    rejectUnauthorized: false, // Adjust based on your PostgreSQL SSL configuration
  },
});

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

// Initialize RabbitMQ Connection
connectRabbitMQ();

// Utility Function for Delay
async function delay(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

// Function to Scrape Twitter
async function scrapeTwitter(query, username, password, email) {
  console.log(`Starting scrape for query: "${query}"`);

  const browser = await puppeteer.launch({
    headless: true, // Set to true for production
    slowMo: 50, // Slow down Puppeteer operations by 50ms
    defaultViewport: null,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const page = await browser.newPage();

  // Increase timeouts to avoid ProtocolError timeouts
  page.setDefaultTimeout(60000);
  page.setDefaultNavigationTimeout(60000);

  // Set a user agent to mimic a real browser
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
      'Chrome/115.0.0.0 Safari/537.36'
  );

  try {
    // Navigate to Twitter login page
    console.log('Navigating to Twitter login page...');
    await page.goto('https://twitter.com/login', { waitUntil: 'networkidle2' });

    // Wait for username input and type username
    console.log('Typing username...');
    await page.waitForSelector('input[name="text"]', { visible: true });
    await page.type('input[name="text"]', username, { delay: 100 });
    await page.keyboard.press('Enter');

    // Wait for password input and type password
    console.log('Typing password...');
    await page.waitForSelector('input[name="password"]', { visible: true });
    await page.type('input[name="password"]', password, { delay: 100 });
    await page.keyboard.press('Enter');

    // Wait for navigation after login
    console.log('Waiting for navigation after login...');
    await page.waitForNavigation({ waitUntil: 'networkidle2' });

    // Check if email prompt is present
    const emailPromptSelector = 'input[name="email"]'; // Update this selector based on Twitter's actual email input field
    const isEmailPrompt = await page.$(emailPromptSelector);

    if (isEmailPrompt) {
      console.log('Email prompt detected. Typing email...');
      await page.type(emailPromptSelector, email, { delay: 100 });
      await page.keyboard.press('Enter');

      // Wait for navigation after email input
      console.log('Waiting for navigation after email input...');
      await page.waitForNavigation({ waitUntil: 'networkidle2' });
    }

    // Verify successful login
    const currentUrl = page.url();
    if (currentUrl.includes('/login')) {
      throw new Error('Login failed. Please check your credentials.');
    }
    console.log('Login successful.');

    // Navigate to search page
    const searchUrl = `https://twitter.com/search?q=${encodeURIComponent(query)}&f=live`;
    console.log(`Navigating to search page: ${searchUrl}`);
    await page.goto(searchUrl, { waitUntil: 'networkidle2' });

    // Wait for tweets to load
    console.log('Waiting for tweets to load...');
    await delay(5000); // Wait 5 seconds

    // Extract tweets
    console.log('Extracting tweets...');
    const tweets = await page.evaluate(() => {
      const tweetNodes = document.querySelectorAll('article[data-testid="tweet"]');
      const data = [];
      let count = 0;
      for (let node of tweetNodes) {
        if (count >= 10) break;

        // Extract tweet text
        const tweetTextNode = node.querySelector('div[lang]');
        const tweetText = tweetTextNode ? tweetTextNode.innerText : '';

        // Extract author handle
        let authorHandle = '';
        const authorLink = node.querySelector('a[href*="/status/"] > div > div > div > span');
        if (authorLink) {
          authorHandle = authorLink.innerText;
        }

        // Extract timestamp
        const timestampNode = node.querySelector('time');
        const timestamp = timestampNode ? timestampNode.getAttribute('datetime') : '';

        // Extract tweet ID
        let tweetId = '';
        const statusLink = node.querySelector('a[href*="/status/"]');
        if (statusLink) {
          const href = statusLink.getAttribute('href');
          const parts = href.split('/status/');
          if (parts.length > 1) {
            tweetId = parts[1].split('?')[0];
          }
        }

        data.push({
          tweet_id: tweetId,
          tweet_text: tweetText,
          author_handle: authorHandle,
          timestamp: timestamp,
        });
        count++;
      }
      return data;
    });

    console.log(`Number of tweets extracted: ${tweets.length}`);
    await browser.close();
    return tweets;
  } catch (error) {
    console.error('Error during scraping:', error);
    await browser.close();
    throw error;
  }
}

// Function to Consume Jobs from RabbitMQ
async function consumeJobs() {
  try {
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
          const tweets = await scrapeTwitter(query, TWITTER_USERNAME, TWITTER_PASSWORD, TWITTER_EMAIL);

          // Insert results into DB
          for (const t of tweets) {
            await pool.query(
              'INSERT INTO results (job_id, tweet_id, tweet_text, author_handle, timestamp) VALUES ($1, $2, $3, $4, $5)',
              [jobId, t.tweet_id, t.tweet_text, t.author_handle, t.timestamp]
            );
          }

          // Update job status to completed
          await pool.query('UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2', [
            'completed',
            jobId,
          ]);
          console.log(`Job ${jobId} completed successfully with ${tweets.length} tweets.`);
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

// Start Consuming Jobs
consumeJobs().catch(console.error);
