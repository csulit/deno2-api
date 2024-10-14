# Deno2 API

A simple RESTful API built with Deno, Hono.js, and PostgreSQL.

## Prerequisites

- Deno 1.32.0 or later
- PostgreSQL database

## Getting Started

1. Clone the repository:

   ```
   git clone https://github.com/yourusername/deno2-api.git
   cd deno2-api
   ```

2. Update the PostgreSQL connection string in `config/postgres.ts`.

3. Run the server:
   ```
   deno task start
   ```

The server will start on `http://localhost:8000` by default.

## Project Structure

- `config/`: Configuration files (e.g., database connection)
- `routes/`: API route definitions
- `handlers/`: Request handlers
- `services/`: Business logic and database queries
- `utils/`: Utility functions and helpers

## Creating Endpoints

To create a new endpoint, follow these steps:

1. Define your route in `routes/index.ts`.
2. Create a handler function in the `handlers/` directory.
3. Implement any necessary business logic and database queries in the
   `services/` directory.

Example of creating a new endpoint:

1. In `routes/index.ts`:

   ```typescript
   import { getUserById } from "../handlers/userHandler.ts";

   // ... existing code ...

   app.get("/users/:id", getUserById);
   ```

2. In `handlers/userHandler.ts`:

   ```typescript
   import { Context } from "@hono/hono";
   import { getUserService } from "../services/userService.ts";

   export const getUserById = async (c: Context) => {
     const userId = c.req.param("id");
     const user = await getUserService(userId);

     if (!user) {
       return c.json({ error: "User not found" }, 404);
     }

     return c.json(user);
   };
   ```

3. In `services/userService.ts`:

   ```typescript
   import client from "../config/postgres.ts";

   export const getUserService = async (userId: string) => {
     const result = await client.queryObject(
       "SELECT * FROM users WHERE id = $1",
       [userId],
     );

     return result.rows[0] || null;
   };
   ```

This example demonstrates how to create a GET endpoint to retrieve a user by ID.
It shows the separation of concerns between routing, handling requests, and
interacting with the database.
