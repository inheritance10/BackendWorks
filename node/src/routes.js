const router = require('express').Router();

router.use('/file', require('./modules/file/file.routes'))

module.exports = router;