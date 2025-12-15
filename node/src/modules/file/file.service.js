const fs = require('node:fs');
const path = require('path');

const filePath = path.join(
    process.cwd(),
    'src/data/files/test.txt'
);

const content = 'Some content!';

exports.write = async () => {
    try {
        await fs.writeFile(filePath, content,
            err => {
                if (err) {
                    console.error(err);
                } else {
                    // file written successfully
                }
            }
        );
        return { status: true }
    } catch (e) {
        throw e;
    }

};

