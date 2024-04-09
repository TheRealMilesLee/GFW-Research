package com.hengyili.ucdavis.edu.androidnotificationpushergfw;

import android.os.Bundle;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;
import android.widget.Toast;
import android.widget.LinearLayout;
import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.cardview.widget.CardView;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import com.google.android.material.floatingactionbutton.FloatingActionButton;

public class MainActivity extends AppCompatActivity
{
  private LinearLayout container;
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

    container = findViewById(R.id.container);
    FloatingActionButton AddNewPostButton = findViewById(R.id.floatingActionButton);
    AddNewPostButton.setOnClickListener(new View.OnClickListener() {
      @Override
      public void onClick(View v)
      {
        addCardViewWithText();
      }
    });
  }
  private void addCardViewWithText() {
    // Create a new CardView
    CardView cardView = new CardView(this);
    cardView.setLayoutParams(new ViewGroup.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));
    cardView.setCardBackgroundColor(getResources().getColor(R.color.md_blue_grey_500));
    cardView.setRadius(getResources().getDimension(R.dimen.card_corner_radius));
    cardView.setCardElevation(getResources().getDimension(R.dimen.card_elevation));

    // Create a new TextView inside the CardView
    TextView textView = new TextView(this);
    textView.setLayoutParams(new ViewGroup.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));
    textView.setText(getString(R.string.new_text_view_text));
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