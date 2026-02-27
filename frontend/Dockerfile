# Frontend (React + Vite) -> build -> Nginx

FROM node:20-alpine AS build
WORKDIR /app

# Cài deps trước để tận dụng cache
COPY package.json package-lock.json ./
RUN npm ci

COPY . ./

# Vite env chỉ có hiệu lực lúc build
ARG VITE_API_BASE=/api
ENV VITE_API_BASE=$VITE_API_BASE

RUN npm run build


FROM nginx:1.25-alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
