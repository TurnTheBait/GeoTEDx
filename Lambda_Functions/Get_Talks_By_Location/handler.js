const connect_to_db = require('./db');
const talk = require('./Talk');

module.exports.get_tag_by_location = async (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  console.log('Received event:', JSON.stringify(event, null, 2));

  const { location } = JSON.parse(event.body);

  let tag = ['general'];

  if (location.toLowerCase() === 'university') {
    tag.push('education');
    tag.push('science');
    tag.push('technology');
    tag.push('personal growth');
    tag.push('innovation');
  } else if (location.toLowerCase() === 'work') {
    tag.push('business');
    tag.push('economics');
    tag.push('comunication');
    tag.push('leadership');
    tag.push('relationships');
    tag.push('society');
    tag.push('goals');
  } else if (location.toLowerCase() === 'home') {
    tag.push('culture');
    tag.push('family');
    tag.push('health');
    tag.push('parenting');
    tag.push('music');
  } else if(location.toLowerCase() ==='gym'){
    tag.push('motivation');
    tag.push('sports');
    tag.push('race');
    tag.push('music');
    tag.push('human body');
  } else if (location.toLowerCase() === 'park') {
    tag.push('nature');
    tag.push('biology');
    tag.push('climate change');
    tag.push('ecology');
    tag.push('environment');
    tag.push('sustainability');
  } else if (location.toLowerCase() === 'bed') {
    tag.push('memory');
    tag.push('sleep');
    tag.push('mental health');
    tag.push('brain');
    tag.push('storytelling');
  } 

  try {
    await module.exports.get_by_tag(tag, callback);
  } catch (error) {
    console.error('Error fetching talks:', error);
    return callback(null, {
      statusCode: 500,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Could not fetch the talks.',
    });
  }
};

module.exports.get_by_tag = async (tag, callback) => {
  try {
    await connect_to_db();
    console.log('=> get_all talks');
    const uniqueTalks = [];
    const usedTags = new Set();

    for (const t of tag) {
      if (!usedTags.has(t)) {
        const talksForTag = await talk.aggregate([
          { $match: { tags: t } },
          { $sample: { size: 1 } },
        ]).exec();

        if (talksForTag.length > 0) {
          uniqueTalks.push(talksForTag[0]);
          usedTags.add(t);
        }
      }
    }

    return callback(null, {
      statusCode: 200,
      body: JSON.stringify(uniqueTalks),
    });

  } catch (error) {
    console.error('Error fetching talks:', error);
    return callback(null, {
      statusCode: error.statusCode || 500,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Could not fetch the talks.',
    });
  }
};
