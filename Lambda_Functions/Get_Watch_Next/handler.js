const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.getwatchnext = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.idx) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. idx is null.'
        })
    }
    
    if (!body.doc_per_page) {
        body.doc_per_page = 10
    }
    if (!body.page) {
        body.page = 1
    }

    connect_to_db().then(() => {
        console.log('=> get_all talks');
        talk.find({_id: body.idx})
            .then(talks => {
                    talk.find({_id: talks[0].watch_next_s})
                    .sort('num_views')
                    .then(t => {
                        callback(null, {
                        statusCode: 200,
                        body: JSON.stringify({id: t[0]._id, title: t[0].title, details: t[0].details, url: t[0].url, num_views: t[0].num_views})
                    })
                })
            })
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.'
                })
            );
    });
};
