const router = require('express').Router();
const controller = require('./file.controller');

router.get('/write', controller.write);

router.get('/test', controller.test);

module.exports = router;