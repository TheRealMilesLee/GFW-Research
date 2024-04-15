package com.hengyili.ucdavis.edu.androidnotificationpushergfw;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.content.SharedPreferences;
import android.net.Uri;
import android.os.Bundle;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;
import androidx.cardview.widget.CardView;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;

public class ProfileActivity extends AppCompatActivity
{
    private ImageView ivAvatar;
    private TextView tvUsername;
    private TextView tvUserInfo;
    public String loadAvatar() {
        SharedPreferences sharedPreferences = getSharedPreferences("Profile", MODE_PRIVATE);
        return sharedPreferences.getString("Avatar", "");
    }

    @SuppressLint("SetTextI18n")
    @Override
    protected void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_profile);

        CardView tvProfile = findViewById(R.id.card_avatar);
        Button btnAddRss = findViewById(R.id.btn_add_rss);
        Button btnBack = findViewById(R.id.btn_back);
        ivAvatar = findViewById(R.id.iv_avatar);
        tvUsername = findViewById(R.id.tv_username);
        tvUserInfo = findViewById(R.id.tv_user_info);

        // Load the profile information
        String[] profile = loadProfile();
        tvUsername.setText(profile[0]);
        tvUserInfo.setText(profile[1] + " | " + profile[2]);

        // Load the avatar from SharedPreferences and set it in the ImageView
        String avatarUri = loadAvatar();
        if (!avatarUri.isEmpty()) {
            Glide.with(this)
              .load(Uri.parse(avatarUri))
              .circleCrop()
              .into(ivAvatar);
        }

        tvProfile.setOnClickListener(v -> {
            // Handle profile click
            ProfileDialogFragment dialog = new ProfileDialogFragment();
            dialog.show(getSupportFragmentManager(), "ProfileDialogFragment");
        });

        btnAddRss.setOnClickListener(v -> {
            // Handle add RSS click
            // Show a dialog or start an activity to add a new RSS source
        });

        btnBack.setOnClickListener(v -> {
            // Handle back click
            // Start MainActivity
            Intent intent = new Intent(ProfileActivity.this, MainActivity.class);
            startActivity(intent);
        });

        // Initialize the RecyclerView to display the RSS sources
        RecyclerView rvRssSources = findViewById(R.id.rv_rss_sources);
        // TODO: Set the RecyclerView adapter with the RSS sources
    }
    public void saveProfile(String name, String email, String phone) {
        SharedPreferences sharedPreferences = getSharedPreferences("Profile", MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString("Name", name);
        editor.putString("Email", email);
        editor.putString("Phone", phone);
        editor.apply();
    }
    public String[] loadProfile() {
        SharedPreferences sharedPreferences = getSharedPreferences("Profile", MODE_PRIVATE);
        String name = sharedPreferences.getString("Name", "");
        String email = sharedPreferences.getString("Email", "");
        String phone = sharedPreferences.getString("Phone", "");
        return new String[]{name, email, phone};
    }
}