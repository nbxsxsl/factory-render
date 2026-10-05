FROM node:24-slim
WORKDIR /app
ENV NODE_ENV=production DB_MODE=sqlite
COPY package.json ./
RUN npm install --omit=dev
COPY server.js sqlite.js gh-backup.js ./
COPY public ./public
EXPOSE 10000
CMD ["node", "--no-warnings", "server.js"]