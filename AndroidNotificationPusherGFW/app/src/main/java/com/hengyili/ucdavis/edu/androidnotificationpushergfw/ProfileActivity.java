package com.hengyili.ucdavis.edu.androidnotificationpushergfw;

import android.content.Intent;
import android.os.Bundle;
import android.widget.Button;
import android.widget.TextView;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.RecyclerView;

public class ProfileActivity extends AppCompatActivity
{
    @Override
    protected void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_profile);

        TextView tvProfile = findViewById(R.id.tv_profile);
        Button btnAddRss = findViewById(R.id.btn_add_rss);
        Button btnBack = findViewById(R.id.btn_back);

        tvProfile.setOnClickListener(v -> {
            // Handle profile click
            // Show a dialog or start an activity to change profile information
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
}