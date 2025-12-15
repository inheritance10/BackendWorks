const service = require('./file.service');
const fs = require('node:fs');
const path = require('path');

const filePath = path.join(
    process.cwd(),
    'src/data/files/test.txt'
);

exports.write = async (req, res, next) => {
    try {
        const result = await service.write();

        return res.status(200).json(result);

    } catch (e) {
        return res.status(500).json({
            success: false,
            message: e.message
        })
    }
}

/*exports.test = async (req, res, next) => {
    try{


        //senkron
        console.log('A');

        //timer phase işlem
        setTimeout(() => {
            console.log('B');
        }, 0);

        //micro taskları node event loopta timerlardan ocnellikli olarak çalışır
        //micro taskler herzaman macro tasktan önce çalışır
        Promise.resolve().then(() => {
            console.log('C');
        })

         //senkron
        console.log('D')

        return res.status(200).json('ok');

    }catch(error){
         return res.status(500).json({
            success: false,
            message: e.message
        })
    }
}*/



/*exports.test = async (req, res, next) => {
    try{

        setTimeout(() => console.log('timeout', 0))

        Promise.resolve().then(() => console.log('promise'));

        process.nextTick(() => console.log('nextTick'));


        return res.status(200).json('ok');

    }catch(error){
         return res.status(500).json({
            success: false,
            message: e.message
        })
    }
}*/


/*exports.test = async (req, res, next) => {
    try {

    
        console.log('1');

        fs.readFile(filePath
             , () => {
                console.log('2 - File Read I/O');

             }
        );

        setTimeout(() => {
            console.log('3 - Timer');
        }, 0);

        console.log('4')



        return res.status(200).json('ok');

    } catch (error) {
        return res.status(500).json({
            success: false,
            message: e.message
        })
    }
}*/



/*exports.test = async (req, res, next) => {
    try {


        console.log('1');

        const data = fs.readFileSync(filePath); //event loop bloklandı
        console.log('2 - Sync read done');

        setTimeout(() => {
            console.log('3 - Timer')
        }, 0);

        console.log('4');



        return res.status(200).json('ok');

    } catch (error) {
        return res.status(500).json({
            success: false,
            message: e.message
        })
    }
}*/

exports.test = async (req, res, next) => {
    try {


        console.log('start');

        for(let i = 0; i < 5e9; i++){
            console.log('aa');
        }

        console.log('end');

        return res.status(200).json('ok');

    } catch (error) {
        return res.status(500).json({
            success: false,
            message: e.message
        })
    }
}