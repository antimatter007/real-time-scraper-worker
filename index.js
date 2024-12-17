const axios = require('axios');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// Example: Retrieve a token beforehand or pass it in as a parameter.
async function getRedditApiToken() {
  // This is a placeholder. You'll need to implement OAuth flow or use saved tokens.
  return process.env.REDDIT_OAUTH_TOKEN || null; 
}

async function fetchFromOfficialApi(query, token) {
  console.log('Attempting Official Reddit API...');
  const url = `https://oauth.reddit.com/search?q=${encodeURIComponent(query)}&limit=10&sort=relevance&t=all`;
  
  try {
    const headers = {
      'User-Agent': 'YourApp/1.0 (by u/YourRedditUsername)',
      Authorization: `bearer ${token}`
    };
    const response = await axios.get(url, { headers });

    if (response.status === 200 && response.data && response.data.data && response.data.data.children.length > 0) {
      console.log(`Official API returned ${response.data.data.children.length} results.`);
      return response.data.data.children.map(post => ({
        tweet_id: post.data.id,
        tweet_text: post.data.title,
        author_handle: post.data.author,
        timestamp: new Date(post.data.created_utc * 1000).toISOString()
      }));
    } else {
      console.warn('Official API returned empty results or unexpected structure.');
      return null;
    }
  } catch (err) {
    console.error('Official API request failed:', err.message);
    return null;
  }
}

async function fetchFromPublicJson(query) {
  console.log('Attempting Public JSON Endpoint...');
  const url = `https://www.reddit.com/search.json?q=${encodeURIComponent(query)}&limit=10&sort=relevance&t=all`;
  
  try {
    const headers = {
      'User-Agent': 'YourApp/1.0 (by u/YourRedditUsername)'
    };
    const response = await axios.get(url, { headers });

    if (response.status === 200 && response.data && response.data.data && response.data.data.children.length > 0) {
      console.log(`Public JSON returned ${response.data.data.children.length} results.`);
      return response.data.data.children.map(post => ({
        tweet_id: post.data.id,
        tweet_text: post.data.title,
        author_handle: post.data.author,
        timestamp: new Date(post.data.created_utc * 1000).toISOString()
      }));
    } else {
      console.warn('Public JSON returned empty results or unexpected structure.');
      return null;
    }
  } catch (err) {
    console.error('Public JSON request failed:', err.message);
    return null;
  }
}

async function scrapeWithPuppeteer(query) {
  console.log('Attempting Puppeteer Scraping...');
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
                          '(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36');

  const url = `https://www.reddit.com/search?q=${encodeURIComponent(query)}&sort=relevance&t=all`;
  await page.goto(url, { waitUntil: 'domcontentloaded' });
  
  // Wait a bit
  await new Promise(r => setTimeout(r, 5000));
  
  // Check if there's captcha
  const htmlContent = await page.content();
  if (htmlContent.includes('are you a human') || htmlContent.includes('captcha')) {
    console.warn('Encountered CAPTCHA during Puppeteer scraping.');
    await browser.close();
    return null;
  }

  try {
    await page.waitForSelector('div[data-testid="search-post-unit"]', { timeout: 60000 });
    const posts = await page.evaluate(() => {
      const postContainers = document.querySelectorAll('div[data-testid="search-post-unit"]');
      const data = [];
      let count = 0;
      for (let container of postContainers) {
        if (count >= 10) break;
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

    console.log(`Puppeteer scraping extracted ${posts.length} posts.`);
    await browser.close();
    return posts;
  } catch (err) {
    console.error('Puppeteer scraping failed:', err.message);
    await browser.close();
    return null;
  }
}

async function getRedditPosts(query) {
  console.log(`Fetching Reddit posts for query: "${query}"`);
  const token = await getRedditApiToken();
  
  let results = null;

  // 1. Try Official API if we have a token
  if (token) {
    results = await fetchFromOfficialApi(query, token);
    if (results && results.length > 0) return results;
  }

  // 2. If Official API fails or no token, try Public JSON
  results = await fetchFromPublicJson(query);
  if (results && results.length > 0) return results;

  // 3. If Public JSON fails, fallback to Puppeteer
  results = await scrapeWithPuppeteer(query);
  if (results && results.length > 0) return results;

  // If everything fails, return empty or throw error
  console.warn('All methods failed to fetch results.');
  return [];
}

// Example usage:
(async () => {
  const posts = await getRedditPosts('twitter');
  console.log('Final Results:', posts);
})();
