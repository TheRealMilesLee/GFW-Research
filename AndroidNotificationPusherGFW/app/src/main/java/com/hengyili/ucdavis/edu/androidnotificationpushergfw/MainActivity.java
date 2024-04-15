package com.hengyili.ucdavis.edu.androidnotificationpushergfw;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.os.Bundle;
import android.view.ViewGroup;
import android.widget.TextView;
import android.widget.LinearLayout;
import androidx.appcompat.app.AppCompatActivity;
import androidx.cardview.widget.CardView;
import com.google.android.material.bottomnavigation.BottomNavigationView;
public class MainActivity extends AppCompatActivity
{
    private LinearLayout container;
    @Override
    protected void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        container = findViewById(R.id.container);
        BottomNavigationView navigation = findViewById(R.id.navigation);
        navigation.setOnNavigationItemSelectedListener(item ->
        {
          if (item.getItemId() == R.id.navigation_home)
          {
            addCardViewWithText();
            return true;
            // Handle other navigation items if needed
          }
          else if (item.getItemId() == R.id.navigation_feature)
          {
              addCardViewWithText();
              return true;
          }
          else if (item.getItemId() == R.id.navigation_search)
          {
              addCardViewWithText();
              return true;
          }
          else if (item.getItemId() == R.id.navigation_settings)
          {
            Intent intent = new Intent(MainActivity.this, ProfileActivity.class);
            startActivity(intent);
            return true;
          }
            return false;
        });
    }

    @SuppressLint("SetTextI18n")
    private void addCardViewWithText() {
        // Create a new CardView
        CardView cardView = new CardView(this);
        cardView.setLayoutParams(new ViewGroup.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));
        cardView.setCardBackgroundColor(getResources().getColor(R.color.md_grey_500));
        cardView.setRadius(getResources().getDimension(R.dimen.card_corner_radius));
        cardView.setCardElevation(getResources().getDimension(R.dimen.card_elevation));

        // Create a new TextView inside the CardView
        TextView textView = new TextView(this);
        textView.setLayoutParams(new ViewGroup.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));
        textView.setText("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed sodales.");
        textView.setPadding(
                getResources().getDimensionPixelSize(R.dimen.card_text_padding),
                getResources().getDimensionPixelSize(R.dimen.card_text_padding),
                getResources().getDimensionPixelSize(R.dimen.card_text_padding),
                getResources().getDimensionPixelSize(R.dimen.card_text_padding)
        );

        // Add the TextView to the CardView
        cardView.addView(textView);

        // Add the CardView to the layout
        container.addView(cardView);
    }
}