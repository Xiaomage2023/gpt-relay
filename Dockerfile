FROM node:20-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY server.js ./
EXPOSE 10000
ENV SERVER_PORT=10000
CMD ["node", "server.js"]
