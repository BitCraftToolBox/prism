FROM node:lts-alpine

WORKDIR /app

COPY package*.json .
RUN npm ci --omit=dev && npm cache clean --force

COPY . .

USER node

ENTRYPOINT ["npx", "tsx", "./generate_feature_jsons.ts"]