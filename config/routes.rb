Rails.application.routes.draw do
  
  resources :posts, only:[:index, :new, :create, :show, :edit]
  devise_for :users, :controllers => { registrations: 'users/registrations' }
 
  root "posts#index"

  devise_scope :user do
    get '/users/sign_out' => 'devise/sessions#destroy'
  end
  # devise_for :users, :controllers => { registrations: 'users/registrations' }

end
