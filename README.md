## Instructions

1. **Clone the Repository**: 
   ```bash
   git clone https://github.com/Ismat-Samadov/Members_Only.git
   ```

2. **Navigate to the Project Directory**: 
   ```bash
   cd Members_Only
   ```

3. **Install Dependencies**: 
   Make sure you have Ruby and Rails installed. Then run:
   ```bash
   bundle install
   ```

4. **Set Up the Database**: 
   ```bash
   rails db:create
   rails db:migrate
   ```

5. **Start the Server**: 
   ```bash
   rails server
   ```

6. **Access the Application**: 
   Open your web browser and go to [http://localhost:3000](http://localhost:3000) to access the application.

## Usage

1. **Sign Up or Log In**: 
   - Navigate to [http://localhost:3000/users/sign_up](http://localhost:3000/users/sign_up) to sign up for a new account.
   - If you already have an account, log in at [http://localhost:3000/users/sign_in](http://localhost:3000/users/sign_in).

2. **Create a Secret Post**: 
   - Once logged in, go to [http://localhost:3000/posts/new](http://localhost:3000/posts/new) to create a new secret post.

3. **View Secret Posts**: 
   - Visit [http://localhost:3000/posts](http://localhost:3000/posts) to view all secret posts.
   - Note that only signed-in users can see the author of each post.

4. **Sign Out**: 
   - To sign out, simply click on the "Sign out" link in the navigation bar.

That's it! You can now use the "Members Only!" application to create and view secret posts. Enjoy!