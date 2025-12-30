from node:20-alpine

WORKDIR /app

COPY package.json .
run npm install

COPY ..

CMD ["node", "index.js"]