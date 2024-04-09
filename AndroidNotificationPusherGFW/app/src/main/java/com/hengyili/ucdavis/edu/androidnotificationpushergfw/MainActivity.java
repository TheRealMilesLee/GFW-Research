package com.hengyili.ucdavis.edu.androidnotificationpushergfw;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.Toast;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

public class MainActivity extends AppCompatActivity
{
  @Override
  protected void onCreate(Bundle savedInstanceState)
  {
    super.onCreate(savedInstanceState);
    EdgeToEdge.enable(this);
    setContentView(R.layout.activity_main);
    ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main), (v, insets) ->
    {
      Insets systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars());
      v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom);
      return insets;
    });
    // Button Behavior goes here
    Button getNotificationButton = findViewById(R.id.GetNotification);
    getNotificationButton.setOnClickListener(new View.OnClickListener() {
      @Override
      public void onClick(View v)
      {
        Toast.makeText(MainActivity.this, "Button Clicked", Toast.LENGTH_SHORT).show();
      }
  });
  }
}